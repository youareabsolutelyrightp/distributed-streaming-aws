package dev.portfolio.taxi.query2;

import static dev.portfolio.taxi.input.InputModels.SemanticTrip;
import static dev.portfolio.taxi.query2.Query2Models.BoundaryMessage;
import static dev.portfolio.taxi.query2.Query2Models.CellDelta;
import static dev.portfolio.taxi.query2.Query2Models.EmptyCountUpdate;
import static dev.portfolio.taxi.query2.Query2Models.EosMessage;
import static dev.portfolio.taxi.query2.Query2Models.FareInput;
import static dev.portfolio.taxi.query2.Query2Models.GlobalMessage;
import static dev.portfolio.taxi.query2.Query2Models.MedianUpdate;
import static dev.portfolio.taxi.query2.Query2Models.ProfitAggregate;
import static dev.portfolio.taxi.query2.Query2Models.ProfitComponentUpdate;
import static dev.portfolio.taxi.query2.Query2Models.ProfitRankEntry;
import static dev.portfolio.taxi.query2.Query2Models.ShardTopMessage;

import dev.portfolio.taxi.model.Grid;
import dev.portfolio.taxi.control.CompletionSignals;
import dev.portfolio.taxi.control.RunControlRecord;
import dev.portfolio.taxi.metrics.AmplificationMetrics;
import dev.portfolio.taxi.metrics.StatefulOperatorMetrics;
import dev.portfolio.taxi.measurement.ResultTransport;
import dev.portfolio.taxi.ranking.RankingMetrics;
import dev.portfolio.taxi.ranking.VirtualShards;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.TreeMap;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class Query2Functions {
    private static final int TIMER_PHASE = 0;
    private static final int INPUT_PHASE = 1;

    private Query2Functions() {}

    public static final class TaxiState
            extends KeyedProcessFunction<String, SemanticTrip, CellDelta>
            implements CheckpointedFunction {
        private transient MapState<Long, TripBucket> pending;
        private transient MapState<Long, TaxiExpiryBucket> expirations;
        private transient ValueState<Integer> currentCell;
        private transient ValueState<Long> currentGeneration;
        private transient ValueState<Long> generation;
        private transient ValueState<Long> lastSourceLine;
        private transient ValueState<String> runId;
        private transient ValueState<String> datasetId;
        private transient Counter taxiDeltas;
        private final StatefulOperatorMetrics stateMetrics = new StatefulOperatorMetrics();

        @Override
        public void open(OpenContext context) {
            pending = map(getRuntimeContext(), "query2-taxi-pending-v1", Long.class, TripBucket.class);
            expirations = map(getRuntimeContext(), "query2-taxi-expirations-v1", Long.class, TaxiExpiryBucket.class);
            currentCell = value(getRuntimeContext(), "query2-taxi-current-cell-v1", Integer.class);
            currentGeneration = value(getRuntimeContext(), "query2-taxi-current-generation-v1", Long.class);
            generation = value(getRuntimeContext(), "query2-taxi-generation-v1", Long.class);
            lastSourceLine = value(getRuntimeContext(), "query2-taxi-last-source-line-v1", Long.class);
            runId = value(getRuntimeContext(), "query2-taxi-run-id-v1", String.class);
            datasetId = value(getRuntimeContext(), "query2-taxi-dataset-id-v1", String.class);
            taxiDeltas = AmplificationMetrics.counter(
                    getRuntimeContext(), "B_taxi_location_deltas");
            stateMetrics.open(getRuntimeContext(), "query2_taxi_state");
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            stateMetrics.initialize(context, "query2-taxi-state-metrics-v1");
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            stateMetrics.snapshot(context);
        }

        @Override
        public void processElement(SemanticTrip trip, Context context, Collector<CellDelta> output)
                throws Exception {
            TripBucket bucket = pending.get(trip.eventTimeMillis());
            if (bucket == null) {
                bucket = new TripBucket();
                stateMetrics.rememberPendingBoundary(trip.eventTimeMillis());
                stateMetrics.addRegisteredEventTimeTimers(1L);
            }
            for (SemanticTrip existing : bucket.values) {
                if (existing.sourceLine() == trip.sourceLine()) return;
            }
            bucket.values.add(trip);
            bucket.bufferedArrivals.merge(stateMetrics.rememberBufferedEvent(), 1, Integer::sum);
            pending.put(trip.eventTimeMillis(), bucket);
            context.timerService().registerEventTimeTimer(trip.eventTimeMillis());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<CellDelta> output)
                throws Exception {
            TaxiExpiryBucket expiryBucket = expirations.get(timestamp);
            if (expiryBucket != null) {
                stateMetrics.addRegisteredEventTimeTimers(-1L);
                expiryBucket.values.sort(Comparator.comparingLong(TaxiExpiry::sourceLine));
                for (TaxiExpiry expiry : expiryBucket.values) {
                    Integer cell = currentCell.value();
                    Long current = currentGeneration.value();
                    if (cell != null && current != null && cell == expiry.cellId()
                            && current == expiry.generation()) {
                        output.collect(delta(cell, -1, timestamp, TIMER_PHASE, expiry.sourceLine(), 0L));
                        currentCell.clear();
                        currentGeneration.clear();
                        stateMetrics.addActiveEntries(-1L);
                    }
                }
                expirations.remove(timestamp);
            }

            TripBucket bucket = pending.get(timestamp);
            if (bucket == null) return;
            stateMetrics.forgetPendingBoundary(timestamp);
            stateMetrics.addRegisteredEventTimeTimers(-1L);
            bucket.values.sort(Comparator.comparingLong(SemanticTrip::sourceLine));
            for (SemanticTrip trip : bucket.values) {
                Long last = lastSourceLine.value();
                if (last != null && trip.sourceLine() <= last) continue;
                rememberIdentity(trip.runId(), trip.datasetId());
                Integer previous = currentCell.value();
                if (previous != null) {
                    output.collect(delta(previous, -1, timestamp, INPUT_PHASE,
                            trip.sourceLine(), 0L));
                    currentCell.clear();
                    currentGeneration.clear();
                    stateMetrics.addActiveEntries(-1L);
                }
                long nextGeneration = orZero(generation.value()) + 1L;
                generation.update(nextGeneration);
                OptionalInt dropoff = Grid.query2Cell(trip.dropoffLongitude(), trip.dropoffLatitude());
                if (dropoff.isPresent()) {
                    int cell = dropoff.getAsInt();
                    currentCell.update(cell);
                    currentGeneration.update(nextGeneration);
                    stateMetrics.addActiveEntries(1L);
                    output.collect(delta(cell, 1, timestamp, INPUT_PHASE,
                            trip.sourceLine(), 0L));
                    long expiryTime = timestamp + ProfitRanking.TAXI_WINDOW_MILLIS;
                    TaxiExpiryBucket future = expirations.get(expiryTime);
                    if (future == null) {
                        future = new TaxiExpiryBucket();
                        stateMetrics.addRegisteredEventTimeTimers(1L);
                    }
                    future.values.add(new TaxiExpiry(trip.sourceLine(), nextGeneration, cell));
                    expirations.put(expiryTime, future);
                    context.timerService().registerEventTimeTimer(expiryTime);
                }
                lastSourceLine.update(trip.sourceLine());
            }
            for (var entry : bucket.bufferedArrivals.entrySet()) {
                stateMetrics.forgetBufferedEvents(entry.getKey(), entry.getValue());
            }
            pending.remove(timestamp);
        }

        private CellDelta delta(int cell, int amount, long time, int phase, long line, long sendMicros)
                throws Exception {
            taxiDeltas.inc();
            return new CellDelta(cell, amount, time, phase, line, runId.value(), datasetId.value(), sendMicros);
        }
        private void rememberIdentity(String candidateRun, String candidateDataset) throws Exception {
            String known = runId.value();
            if (known == null) {
                runId.update(candidateRun);
                datasetId.update(candidateDataset);
            } else if (!known.equals(candidateRun) || !datasetId.value().equals(candidateDataset)) {
                throw new IllegalArgumentException("taxi state mixed run or dataset identities");
            }
        }
    }

    public static final class EmptyCellState
            extends KeyedProcessFunction<Integer, CellDelta, ProfitComponentUpdate>
            implements CheckpointedFunction {
        private transient MapState<Long, DeltaBucket> pending;
        private transient ValueState<Long> count;
        private final StatefulOperatorMetrics stateMetrics = new StatefulOperatorMetrics();

        @Override
        public void open(OpenContext context) {
            pending = map(getRuntimeContext(), "query2-empty-pending-v1", Long.class, DeltaBucket.class);
            count = value(getRuntimeContext(), "query2-empty-count-v1", Long.class);
            stateMetrics.open(getRuntimeContext(), "query2_empty_cell_state");
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            stateMetrics.initialize(context, "query2-empty-cell-state-metrics-v1");
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            stateMetrics.snapshot(context);
        }

        @Override
        public void processElement(CellDelta delta, Context context, Collector<ProfitComponentUpdate> output)
                throws Exception {
            DeltaBucket bucket = pending.get(delta.effectiveTimeMillis());
            if (bucket == null) {
                bucket = new DeltaBucket();
                stateMetrics.rememberPendingBoundary(delta.effectiveTimeMillis());
                stateMetrics.addRegisteredEventTimeTimers(1L);
            }
            bucket.values.add(delta);
            pending.put(delta.effectiveTimeMillis(), bucket);
            context.timerService().registerEventTimeTimer(delta.effectiveTimeMillis());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<ProfitComponentUpdate> output)
                throws Exception {
            DeltaBucket bucket = pending.get(timestamp);
            if (bucket == null) return;
            stateMetrics.forgetPendingBoundary(timestamp);
            stateMetrics.addRegisteredEventTimeTimers(-1L);
            bucket.values.sort(Comparator.comparingInt(CellDelta::phase)
                    .thenComparingLong(CellDelta::sourceLine)
                    .thenComparingInt(CellDelta::delta));
            long updated = orZero(count.value());
            int latestPhase = TIMER_PHASE;
            long latestLine = 0L;
            long sendMicros = 0L;
            boolean input = false;
            boolean timer = false;
            CellDelta identity = null;
            for (CellDelta delta : bucket.values) {
                updated += delta.delta();
                if (updated < 0L) throw new IllegalStateException("empty-taxi count became negative");
                input |= delta.phase() == INPUT_PHASE;
                timer |= delta.phase() == TIMER_PHASE;
                if (later(delta.phase(), delta.sourceLine(), latestPhase, latestLine)) {
                    latestPhase = delta.phase();
                    latestLine = delta.sourceLine();
                }
                sendMicros = Math.max(sendMicros, delta.producerSendEpochMicros());
                identity = delta;
            }
            boolean wasActive = count.value() != null;
            if (!wasActive && updated > 0L) {
                stateMetrics.addActiveEntries(1L);
            }
            output.collect(new EmptyCountUpdate(
                    context.getCurrentKey(), updated, timestamp, input, timer, latestPhase, latestLine,
                    identity.runId(), identity.datasetId(), sendMicros));
            if (updated == 0L) {
                count.clear();
                if (wasActive) {
                    stateMetrics.addActiveEntries(-1L);
                }
            } else {
                count.update(updated);
            }
            pending.remove(timestamp);
        }
    }

    public static final class FareWindow
            extends KeyedProcessFunction<Integer, FareInput, ProfitComponentUpdate>
            implements CheckpointedFunction {
        private transient MapState<Long, FareBucket> pending;
        private transient MapState<Long, FareExpiryBucket> expirations;
        private transient ValueState<ExactMedianWindow> windowState;
        private transient Counter medianUpdates;
        private final StatefulOperatorMetrics stateMetrics = new StatefulOperatorMetrics();

        @Override
        public void open(OpenContext context) {
            pending = map(getRuntimeContext(), "query2-fare-pending-v1", Long.class, FareBucket.class);
            expirations = map(getRuntimeContext(), "query2-fare-expirations-v1", Long.class, FareExpiryBucket.class);
            windowState = value(getRuntimeContext(), "query2-fare-window-v1", ExactMedianWindow.class);
            medianUpdates = AmplificationMetrics.counter(
                    getRuntimeContext(), "C_fare_median_updates");
            stateMetrics.open(getRuntimeContext(), "query2_fare_window");
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            stateMetrics.initialize(context, "query2-fare-state-metrics-v1");
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            stateMetrics.snapshot(context);
        }

        @Override
        public void processElement(FareInput input, Context context, Collector<ProfitComponentUpdate> output)
                throws Exception {
            FareBucket bucket = pending.get(input.eventTimeMillis());
            if (bucket == null) {
                bucket = new FareBucket();
                stateMetrics.rememberPendingBoundary(input.eventTimeMillis());
                stateMetrics.addRegisteredEventTimeTimers(1L);
            }
            for (FareInput existing : bucket.values) if (existing.sourceLine() == input.sourceLine()) return;
            bucket.values.add(input);
            bucket.bufferedArrivals.merge(stateMetrics.rememberBufferedEvent(), 1, Integer::sum);
            pending.put(input.eventTimeMillis(), bucket);
            context.timerService().registerEventTimeTimer(input.eventTimeMillis());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<ProfitComponentUpdate> output)
                throws Exception {
            ExactMedianWindow window = windowState.value();
            if (window == null) window = new ExactMedianWindow();
            boolean inputChanged = false;
            boolean timerChanged = false;
            int latestPhase = TIMER_PHASE;
            long latestLine = 0L;
            long sendMicros = 0L;
            String run = null;
            String dataset = null;

            FareExpiryBucket expired = expirations.get(timestamp);
            if (expired != null) {
                stateMetrics.addRegisteredEventTimeTimers(-1L);
                expired.values.sort(Comparator.comparingLong(FareExpiry::sourceLine));
                for (FareExpiry expiry : expired.values) {
                    window.remove(expiry.moneyCents());
                    stateMetrics.addActiveEntries(-1L);
                    timerChanged = true;
                    latestLine = Math.max(latestLine, expiry.sourceLine());
                    run = expiry.runId();
                    dataset = expiry.datasetId();
                }
                expirations.remove(timestamp);
            }
            FareBucket inputs = pending.get(timestamp);
            if (inputs != null) {
                stateMetrics.forgetPendingBoundary(timestamp);
                stateMetrics.addRegisteredEventTimeTimers(-1L);
                inputs.values.sort(Comparator.comparingLong(FareInput::sourceLine));
                for (FareInput input : inputs.values) {
                    window.add(input.moneyCents());
                    stateMetrics.addActiveEntries(1L);
                    long expiryTime = timestamp + ProfitRanking.FARE_WINDOW_MILLIS;
                    FareExpiryBucket future = expirations.get(expiryTime);
                    if (future == null) {
                        future = new FareExpiryBucket();
                        stateMetrics.addRegisteredEventTimeTimers(1L);
                    }
                    future.values.add(new FareExpiry(input.sourceLine(), input.moneyCents(), input.runId(), input.datasetId()));
                    expirations.put(expiryTime, future);
                    context.timerService().registerEventTimeTimer(expiryTime);
                    inputChanged = true;
                    latestPhase = INPUT_PHASE;
                    latestLine = input.sourceLine();
                    sendMicros = Math.max(sendMicros, input.producerSendEpochMicros());
                    run = input.runId();
                    dataset = input.datasetId();
                }
                for (var entry : inputs.bufferedArrivals.entrySet()) {
                    stateMetrics.forgetBufferedEvents(entry.getKey(), entry.getValue());
                }
                pending.remove(timestamp);
            }
            if (!inputChanged && !timerChanged) return;
            output.collect(new MedianUpdate(
                    context.getCurrentKey(), window.size() == 0L ? 0L : window.medianTwiceCents(),
                    window.size() > 0L, timestamp, inputChanged, timerChanged, latestPhase, latestLine,
                    run, dataset, sendMicros));
            if (window.size() == 0L) {
                windowState.clear();
            } else {
                windowState.update(window);
            }
            medianUpdates.inc();
        }
    }

    public static final class ProfitCellState
            extends KeyedProcessFunction<Integer, ProfitComponentUpdate, ProfitAggregate>
            implements CheckpointedFunction {
        private transient MapState<Long, ComponentBucket> pending;
        private transient ValueState<Long> emptyTaxis;
        private transient ValueState<Long> medianTwice;
        private transient ValueState<Boolean> medianActive;
        private transient Counter profitabilityUpdates;
        private final StatefulOperatorMetrics stateMetrics = new StatefulOperatorMetrics();

        @Override
        public void open(OpenContext context) {
            pending = map(getRuntimeContext(), "query2-profit-pending-v1", Long.class, ComponentBucket.class);
            emptyTaxis = value(getRuntimeContext(), "query2-profit-empty-v1", Long.class);
            medianTwice = value(getRuntimeContext(), "query2-profit-median-v1", Long.class);
            medianActive = value(getRuntimeContext(), "query2-profit-median-active-v1", Boolean.class);
            profitabilityUpdates = AmplificationMetrics.counter(
                    getRuntimeContext(), "D_profitability_updates");
            stateMetrics.open(getRuntimeContext(), "query2_profit_cell_state");
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            stateMetrics.initialize(context, "query2-profit-cell-state-metrics-v1");
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            stateMetrics.snapshot(context);
        }

        @Override
        public void processElement(ProfitComponentUpdate update, Context context, Collector<ProfitAggregate> output)
                throws Exception {
            ComponentBucket bucket = pending.get(update.effectiveTimeMillis());
            if (bucket == null) {
                bucket = new ComponentBucket();
                stateMetrics.rememberPendingBoundary(update.effectiveTimeMillis());
                stateMetrics.addRegisteredEventTimeTimers(1L);
            }
            bucket.values.add(update);
            pending.put(update.effectiveTimeMillis(), bucket);
            context.timerService().registerEventTimeTimer(update.effectiveTimeMillis());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<ProfitAggregate> output)
                throws Exception {
            ComponentBucket bucket = pending.get(timestamp);
            if (bucket == null) return;
            stateMetrics.forgetPendingBoundary(timestamp);
            stateMetrics.addRegisteredEventTimeTimers(-1L);
            bucket.values.sort(Comparator.comparingInt(ProfitComponentUpdate::phase)
                    .thenComparingLong(ProfitComponentUpdate::freshnessSourceLine));
            int latestPhase = TIMER_PHASE;
            long latestLine = 0L;
            long sendMicros = 0L;
            boolean input = false;
            boolean timer = false;
            String run = null;
            String dataset = null;
            boolean hadEmptyState = emptyTaxis.value() != null;
            boolean hadMedianState = medianActive.value() != null;
            long nextEmpty = orZero(emptyTaxis.value());
            long nextMedianTwice = medianTwice.value() == null ? 0L : medianTwice.value();
            boolean nextMedianActive = Boolean.TRUE.equals(medianActive.value());
            for (ProfitComponentUpdate update : bucket.values) {
                if (update instanceof EmptyCountUpdate empty) {
                    nextEmpty = empty.emptyTaxis();
                }
                else if (update instanceof MedianUpdate median) {
                    nextMedianTwice = median.medianTwiceCents();
                    nextMedianActive = median.active();
                }
                input |= update instanceof EmptyCountUpdate empty ? empty.input() : ((MedianUpdate) update).input();
                timer |= update instanceof EmptyCountUpdate empty ? empty.timer() : ((MedianUpdate) update).timer();
                if (later(update.phase(), update.freshnessSourceLine(), latestPhase, latestLine)) {
                    latestPhase = update.phase();
                    latestLine = update.freshnessSourceLine();
                }
                sendMicros = Math.max(sendMicros, update.producerSendEpochMicros());
                run = update.runId();
                dataset = update.datasetId();
            }
            boolean active = nextEmpty > 0L && nextMedianActive;
            output.collect(new ProfitAggregate(
                    context.getCurrentKey(), active ? nextMedianTwice : 0L, nextEmpty,
                    timestamp, latestLine, timestamp, input, timer, run, dataset, sendMicros, active));
            if (nextEmpty == 0L) {
                emptyTaxis.clear();
                if (hadEmptyState) {
                    stateMetrics.addActiveEntries(-1L);
                }
            } else {
                emptyTaxis.update(nextEmpty);
                if (!hadEmptyState) {
                    stateMetrics.addActiveEntries(1L);
                }
            }
            if (nextMedianActive) {
                medianTwice.update(nextMedianTwice);
                medianActive.update(true);
                if (!hadMedianState) {
                    stateMetrics.addActiveEntries(1L);
                }
            } else {
                medianTwice.clear();
                medianActive.clear();
                if (hadMedianState) {
                    stateMetrics.addActiveEntries(-1L);
                }
            }
            profitabilityUpdates.inc();
            pending.remove(timestamp);
        }
    }

    public static final class ShardRank
            extends KeyedProcessFunction<Integer, ProfitAggregate, GlobalMessage>
            implements CheckpointedFunction {
        private transient ValueState<ProfitRanking> rankingState;
        private transient ValueState<String> signatureState;
        private transient MapState<Long, ProfitBucket> pending;
        private transient Counter rankingInputs;
        private transient Counter shardSnapshots;
        private transient RankingMetrics rankingMetrics;
        private final StatefulOperatorMetrics stateMetrics = new StatefulOperatorMetrics();

        @Override
        public void open(OpenContext context) {
            rankingState = value(getRuntimeContext(), "query2-shard-ranking-v1", ProfitRanking.class);
            signatureState = value(getRuntimeContext(), "query2-shard-signature-v1", String.class);
            pending = map(getRuntimeContext(), "query2-shard-pending-v1", Long.class, ProfitBucket.class);
            rankingInputs = AmplificationMetrics.counter(
                    getRuntimeContext(), "E_shard_ranking_inputs");
            shardSnapshots = AmplificationMetrics.counter(
                    getRuntimeContext(), "F_changed_shard_snapshots");
            rankingMetrics = new RankingMetrics(getRuntimeContext(), "query2");
            stateMetrics.open(getRuntimeContext(), "query2_shard_rank");
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            stateMetrics.initialize(context, "query2-shard-state-metrics-v1");
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            stateMetrics.snapshot(context);
        }

        @Override
        public void processElement(ProfitAggregate value, Context context, Collector<GlobalMessage> output)
                throws Exception {
            rankingInputs.inc();
            rankingMetrics.input(context.getCurrentKey());
            ProfitBucket bucket = pending.get(value.effectiveTimeMillis());
            if (bucket == null) {
                bucket = new ProfitBucket();
                stateMetrics.rememberPendingBoundary(value.effectiveTimeMillis());
                stateMetrics.addRegisteredEventTimeTimers(1L);
            }
            bucket.values.add(value);
            pending.put(value.effectiveTimeMillis(), bucket);
            context.timerService().registerEventTimeTimer(value.effectiveTimeMillis());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<GlobalMessage> output)
                throws Exception {
            ProfitBucket bucket = pending.get(timestamp);
            if (bucket == null) return;
            stateMetrics.forgetPendingBoundary(timestamp);
            stateMetrics.addRegisteredEventTimeTimers(-1L);
            ProfitRanking ranking = rankingState.value();
            if (ranking == null) ranking = new ProfitRanking();
            String run = null;
            String dataset = null;
            long sendMicros = 0L;
            for (ProfitAggregate value : bucket.values) {
                if (value.active()) {
                    if (!ranking.contains(value.cellId())) {
                        stateMetrics.addActiveEntries(1L);
                    }
                    ranking.set(new ProfitRankEntry(
                            value.cellId(), value.medianTwiceCents(), value.emptyTaxis(),
                            value.freshnessTimeMillis(), value.freshnessSourceLine()));
                } else {
                    if (ranking.contains(value.cellId())) {
                        stateMetrics.addActiveEntries(-1L);
                    }
                    ranking.remove(value.cellId());
                }
                run = value.runId();
                dataset = value.datasetId();
                sendMicros = Math.max(sendMicros, value.producerSendEpochMicros());
            }
            String signature = ProfitRanking.signature(ranking.top());
            if (!signature.equals(signatureState.value())) {
                output.collect(new ShardTopMessage(context.getCurrentKey(), ranking.top(), timestamp,
                        run, dataset, sendMicros));
                shardSnapshots.inc();
                rankingMetrics.snapshot();
                signatureState.update(signature);
            }
            rankingState.update(ranking);
            pending.remove(timestamp);
        }
    }

    public static final class GlobalRank
            extends KeyedProcessFunction<Integer, GlobalMessage, Query2Result>
            implements CheckpointedFunction {
        private transient MapState<Integer, List<ProfitRankEntry>> shardTops;
        private transient MapState<Long, BoundaryFlags> boundaries;
        private transient MapState<Integer, Boolean> eosPartitions;
        private transient ValueState<String> lastSignature;
        private transient ValueState<Long> terminalBoundary;
        private transient ValueState<Boolean> terminalComplete;
        private final int rankingShards;
        private transient Counter globalInputs;
        private transient Counter globalBoundaryInputs;
        private transient Counter globalShardInputs;
        private transient Counter globalEosInputs;
        private transient Counter finalizedBoundaries;
        private transient Counter emittedChanges;
        private final StatefulOperatorMetrics stateMetrics = new StatefulOperatorMetrics();

        public GlobalRank() {
            this(64);
        }

        public GlobalRank(int rankingShards) {
            if (rankingShards <= 0) {
                throw new IllegalArgumentException("ranking shards must be positive");
            }
            this.rankingShards = rankingShards;
        }

        @Override
        public void open(OpenContext context) {
            shardTops = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                    "query2-global-shard-tops-v1", TypeInformation.of(Integer.class),
                    TypeInformation.of(new TypeHint<List<ProfitRankEntry>>() {})));
            boundaries = map(
                    getRuntimeContext(),
                    "query2-global-boundaries-v3",
                    Long.class,
                    BoundaryFlags.class);
            eosPartitions = map(getRuntimeContext(), "query2-global-eos-v1", Integer.class, Boolean.class);
            lastSignature = value(getRuntimeContext(), "query2-global-signature-v1", String.class);
            terminalBoundary = value(getRuntimeContext(), "query2-global-terminal-boundary-v1", Long.class);
            terminalComplete = value(getRuntimeContext(), "query2-global-terminal-complete-v1", Boolean.class);
            globalInputs = AmplificationMetrics.counter(
                    getRuntimeContext(), "G_global_ranking_inputs");
            globalBoundaryInputs = AmplificationMetrics.counter(
                    getRuntimeContext(), "G_global_shared_boundaries");
            globalShardInputs = AmplificationMetrics.counter(
                    getRuntimeContext(), "G_global_shard_snapshots");
            globalEosInputs = AmplificationMetrics.counter(
                    getRuntimeContext(), "G_global_eos");
            finalizedBoundaries = AmplificationMetrics.counter(
                    getRuntimeContext(), "H_finalized_boundaries");
            emittedChanges = AmplificationMetrics.counter(
                    getRuntimeContext(), "I_emitted_top_ten_changes");
            stateMetrics.open(getRuntimeContext(), "query2_global_rank");
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            stateMetrics.initialize(context, "query2-global-state-metrics-v3");
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            stateMetrics.snapshot(context);
        }

        @Override
        public void processElement(GlobalMessage value, Context context, Collector<Query2Result> output)
                throws Exception {
            globalInputs.inc();
            Long terminal = terminalBoundary.value();
            if (Boolean.TRUE.equals(terminalComplete.value()) && terminal != null
                    && value.effectiveTimeMillis() > terminal) {
                return;
            }
            BoundaryFlags flags = boundaries.get(value.effectiveTimeMillis());
            if (flags == null) {
                flags = new BoundaryFlags();
                stateMetrics.rememberPendingBoundary(value.effectiveTimeMillis());
                stateMetrics.addRegisteredEventTimeTimers(1L);
            }
            if (value instanceof ShardTopMessage shard) {
                globalShardInputs.inc();
                flags.shardUpdates.put(shard.shardId(), shard.top());
                flags.rememberIdentity(shard.runId(), shard.datasetId());
                flags.sendMicros = Math.max(flags.sendMicros, shard.producerSendEpochMicros());
                flags.rememberRequiredArrival(ResultTransport.nowEpochMicros());
            } else if (value instanceof BoundaryMessage boundary) {
                globalBoundaryInputs.inc();
                flags.input |= boundary.input();
                flags.timer |= boundary.timer();
                flags.rememberIdentity(boundary.runId(), boundary.datasetId());
                flags.sendMicros = Math.max(flags.sendMicros, boundary.producerSendEpochMicros());
                flags.rememberTransport(boundary, ResultTransport.nowEpochMicros());
            } else if (value instanceof EosMessage eos) {
                globalEosInputs.inc();
                if (!eosPartitions.contains(eos.partition())) {
                    stateMetrics.addActiveEntries(1L);
                }
                eosPartitions.put(eos.partition(), true);
                flags.rememberIdentity(eos.runId(), eos.datasetId());
                int observed = 0;
                for (Boolean ignored : eosPartitions.values()) observed++;
                if (observed >= eos.expectedPartitionCount()) {
                    flags.terminal = true;
                    terminalBoundary.update(eos.effectiveTimeMillis());
                }
            }
            boundaries.put(value.effectiveTimeMillis(), flags);
            context.timerService().registerEventTimeTimer(value.effectiveTimeMillis());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Query2Result> output)
                throws Exception {
            Long terminal = terminalBoundary.value();
            if (terminal != null && timestamp > terminal) {
                if (boundaries.contains(timestamp)) {
                    boundaries.remove(timestamp);
                    stateMetrics.forgetPendingBoundary(timestamp);
                    stateMetrics.addRegisteredEventTimeTimers(-1L);
                }
                return;
            }
            BoundaryFlags flags = boundaries.get(timestamp);
            if (flags == null) return;
            finalizedBoundaries.inc();
            long watermarkClosedEpochMicros = ResultTransport.nowEpochMicros();
            stateMetrics.forgetPendingBoundary(timestamp);
            stateMetrics.addRegisteredEventTimeTimers(-1L);
            for (Map.Entry<Integer, List<ProfitRankEntry>> update : flags.shardUpdates.entrySet()) {
                List<ProfitRankEntry> previousTop = shardTops.get(update.getKey());
                if (previousTop != null) {
                    stateMetrics.addActiveEntries(-previousTop.size());
                }
                shardTops.put(update.getKey(), update.getValue());
                stateMetrics.addActiveEntries(update.getValue().size());
            }
            ArrayList<List<ProfitRankEntry>> tops = new ArrayList<>(rankingShards);
            for (int shard = 0; shard < rankingShards; shard++) {
                List<ProfitRankEntry> shardTop = shardTops.get(shard);
                if (shardTop != null && !shardTop.isEmpty()) {
                    tops.add(shardTop);
                }
            }
            long globalBoundaryStartedEpochMicros = ResultTransport.nowEpochMicros();
            List<ProfitRankEntry> top = GlobalTopTenMerger.merge(tops);
            String signature = ProfitRanking.signature(top);
            String previous = lastSignature.value();
            if (flags.terminal || previous == null || !signature.equals(previous)) {
                String trigger = flags.input ? (flags.timer ? "mixed" : "input")
                        : (flags.timer ? "event_time_timer" : "terminal");
                long resultEmittedEpochMicros = ResultTransport.nowEpochMicros();
                ResultTransport transport = flags.transport(
                        watermarkClosedEpochMicros,
                        globalBoundaryStartedEpochMicros,
                        resultEmittedEpochMicros);
                Query2Result result = new Query2Result(
                        timestamp, trigger, null, null, flags.terminal, top,
                        flags.runId, flags.datasetId, transport);
                output.collect(result);
                emittedChanges.inc();
                if (flags.terminal) {
                    context.output(
                            CompletionSignals.QUERY2,
                            RunControlRecord.queryComplete(
                                    "query2",
                                    timestamp,
                                    flags.runId,
                                    flags.datasetId,
                                    result.outputId()));
                }
                lastSignature.update(signature);
            }
            if (flags.terminal) terminalComplete.update(true);
            boundaries.remove(timestamp);
        }

    }


    public static FareInput fareInput(SemanticTrip trip) {
        OptionalInt pickup = Grid.query2Cell(trip.pickupLongitude(), trip.pickupLatitude());
        Long money = Query2Money.farePlusTipCents(trip.fareAmount(), trip.tipAmount());
        if (pickup.isEmpty() || money == null) return null;
        return new FareInput(pickup.getAsInt(), money, trip.eventTimeMillis(), trip.sourceLine(),
                trip.runId(), trip.datasetId(), 0L);
    }

    public static int rankingShard(int cellId, int shards) {
        return VirtualShards.shard(Integer.toUnsignedLong(cellId), shards);
    }

    private static boolean later(int phase, long line, int knownPhase, long knownLine) {
        return phase > knownPhase || (phase == knownPhase && line > knownLine);
    }
    private static long orZero(Long value) { return value == null ? 0L : value; }
    private static <T> ValueState<T> value(RuntimeContext context, String name, Class<T> type) {
        return context.getState(new ValueStateDescriptor<>(name, type));
    }
    private static <K, V> MapState<K, V> map(
            RuntimeContext context, String name, Class<K> key, Class<V> value) {
        return context.getMapState(new MapStateDescriptor<>(name, key, value));
    }

    public static final class TripBucket implements Serializable {
        final List<SemanticTrip> values = new ArrayList<>();
        final TreeMap<Long, Integer> bufferedArrivals = new TreeMap<>();
    }
    public static final class TaxiExpiryBucket implements Serializable { final List<TaxiExpiry> values = new ArrayList<>(); }
    public record TaxiExpiry(long sourceLine, long generation, int cellId) implements Serializable {}
    public static final class DeltaBucket implements Serializable { final List<CellDelta> values = new ArrayList<>(); }
    public static final class FareBucket implements Serializable {
        final List<FareInput> values = new ArrayList<>();
        final TreeMap<Long, Integer> bufferedArrivals = new TreeMap<>();
    }
    public static final class FareExpiryBucket implements Serializable { final List<FareExpiry> values = new ArrayList<>(); }
    public record FareExpiry(long sourceLine, long moneyCents, String runId, String datasetId) implements Serializable {}

    public static final class ComponentBucket implements Serializable { final List<ProfitComponentUpdate> values = new ArrayList<>(); }
    public static final class ProfitBucket implements Serializable { final List<ProfitAggregate> values = new ArrayList<>(); }
    public static final class BoundaryFlags implements Serializable {
        boolean input;
        boolean timer;
        boolean terminal;
        String runId;
        String datasetId;
        long sendMicros;
        long inputKafkaAppendEpochMicros;
        long flinkSourceReadEpochMicros;
        long boundaryFirstInputAppendEpochMicros = Long.MAX_VALUE;
        int triggerInputPartition = -1;
        long triggerInputSourceLine;
        long triggerInputSourceReadEpochMicros;
        long partitionBoundaryEmittedEpochMicros;
        long firstInputBoundaryArrivalEpochMicros = Long.MAX_VALUE;
        long lastInputBoundaryArrivalEpochMicros;
        long lastRequiredGlobalMessageArrivalEpochMicros;
        long maximumPartitionBoundaryDeliveryDurationMicros;
        final Map<Integer, List<ProfitRankEntry>> shardUpdates = new HashMap<>();
        void rememberIdentity(String candidateRun, String candidateDataset) {
            if (candidateRun == null) return;
            if (runId == null) {
                runId = candidateRun;
                datasetId = candidateDataset;
            } else if (!runId.equals(candidateRun) || !datasetId.equals(candidateDataset)) {
                throw new IllegalArgumentException("global Query 2 state mixed run or dataset identities");
            }
        }

        void rememberTransport(BoundaryMessage boundary, long arrivalEpochMicros) {
            long effectiveArrival = Math.max(
                    Math.max(arrivalEpochMicros, boundary.boundarySignalArrivalEpochMicros()),
                    boundary.partitionBoundaryEmittedEpochMicros());
            if (boundary.input()) {
                long firstAppend = boundary.boundaryFirstInputAppendEpochMicros();
                if (firstAppend > 0L) {
                    boundaryFirstInputAppendEpochMicros = Math.min(
                            boundaryFirstInputAppendEpochMicros, firstAppend);
                }
                if (boundary.inputKafkaAppendEpochMicros() > inputKafkaAppendEpochMicros
                        || (boundary.inputKafkaAppendEpochMicros() == inputKafkaAppendEpochMicros
                                && boundary.triggerInputSourceLine() > triggerInputSourceLine)) {
                    inputKafkaAppendEpochMicros = boundary.inputKafkaAppendEpochMicros();
                    triggerInputPartition = boundary.triggerInputPartition();
                    triggerInputSourceLine = boundary.triggerInputSourceLine();
                    triggerInputSourceReadEpochMicros =
                            boundary.triggerInputSourceReadEpochMicros();
                }
                long sharedEmission = boundary.sharedBoundaryEmittedEpochMicros();
                if (sharedEmission > 0L) {
                    firstInputBoundaryArrivalEpochMicros = Math.min(
                            firstInputBoundaryArrivalEpochMicros, sharedEmission);
                    lastInputBoundaryArrivalEpochMicros = Math.max(
                            lastInputBoundaryArrivalEpochMicros, sharedEmission);
                }
            }
            flinkSourceReadEpochMicros = Math.max(
                    flinkSourceReadEpochMicros, boundary.flinkSourceReadEpochMicros());
            partitionBoundaryEmittedEpochMicros = Math.max(
                    partitionBoundaryEmittedEpochMicros,
                    boundary.partitionBoundaryEmittedEpochMicros());
            rememberRequiredArrival(effectiveArrival);
            if (effectiveArrival > 0L && boundary.partitionBoundaryEmittedEpochMicros() > 0L) {
                maximumPartitionBoundaryDeliveryDurationMicros = Math.max(
                        maximumPartitionBoundaryDeliveryDurationMicros,
                        Math.max(
                                0L,
                                effectiveArrival
                                        - boundary.partitionBoundaryEmittedEpochMicros()));
            }
        }

        void rememberRequiredArrival(long arrivalEpochMicros) {
            lastRequiredGlobalMessageArrivalEpochMicros = Math.max(
                    lastRequiredGlobalMessageArrivalEpochMicros, arrivalEpochMicros);
        }

        ResultTransport transport(
                long watermarkClosedEpochMicros,
                long globalBoundaryStartedEpochMicros,
                long resultEmittedEpochMicros) {
            if (!input) {
                return new ResultTransport(
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        partitionBoundaryEmittedEpochMicros == 0L
                                ? null
                                : partitionBoundaryEmittedEpochMicros,
                        firstInputBoundaryArrivalEpochMicros == Long.MAX_VALUE
                                ? null
                                : firstInputBoundaryArrivalEpochMicros,
                        lastInputBoundaryArrivalEpochMicros == 0L
                                ? null
                                : lastInputBoundaryArrivalEpochMicros,
                        lastRequiredGlobalMessageArrivalEpochMicros == 0L
                                ? null
                                : lastRequiredGlobalMessageArrivalEpochMicros,
                        watermarkClosedEpochMicros,
                        globalBoundaryStartedEpochMicros,
                        resultEmittedEpochMicros,
                        maximumPartitionBoundaryDeliveryDurationMicros == 0L
                                ? null
                                : maximumPartitionBoundaryDeliveryDurationMicros);
            }
            return new ResultTransport(
                    boundaryFirstInputAppendEpochMicros == Long.MAX_VALUE
                            ? null
                            : boundaryFirstInputAppendEpochMicros,
                    inputKafkaAppendEpochMicros == 0L ? null : inputKafkaAppendEpochMicros,
                    triggerInputPartition < 0 ? null : triggerInputPartition,
                    triggerInputSourceLine == 0L ? null : triggerInputSourceLine,
                    triggerInputSourceReadEpochMicros == 0L
                            ? null
                            : triggerInputSourceReadEpochMicros,
                    flinkSourceReadEpochMicros == 0L ? null : flinkSourceReadEpochMicros,
                    partitionBoundaryEmittedEpochMicros == 0L
                            ? null
                            : partitionBoundaryEmittedEpochMicros,
                    firstInputBoundaryArrivalEpochMicros == Long.MAX_VALUE
                            ? null
                            : firstInputBoundaryArrivalEpochMicros,
                    lastInputBoundaryArrivalEpochMicros == 0L
                            ? null
                            : lastInputBoundaryArrivalEpochMicros,
                    lastRequiredGlobalMessageArrivalEpochMicros == 0L
                            ? null
                            : lastRequiredGlobalMessageArrivalEpochMicros,
                    watermarkClosedEpochMicros,
                    globalBoundaryStartedEpochMicros,
                    resultEmittedEpochMicros,
                    maximumPartitionBoundaryDeliveryDurationMicros == 0L
                            ? null
                            : maximumPartitionBoundaryDeliveryDurationMicros);
        }
    }
}
