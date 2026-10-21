package dev.portfolio.taxi.query1;

import static dev.portfolio.taxi.query1.Query1Models.BoundaryMessage;
import static dev.portfolio.taxi.query1.Query1Models.EosMessage;
import static dev.portfolio.taxi.query1.Query1Models.GlobalMessage;
import static dev.portfolio.taxi.query1.Query1Models.RouteAggregate;
import static dev.portfolio.taxi.query1.Query1Models.RouteRankEntry;
import static dev.portfolio.taxi.query1.Query1Models.RouteUpdate;
import static dev.portfolio.taxi.query1.Query1Models.ShardTopMessage;

import dev.portfolio.taxi.control.CompletionSignals;
import dev.portfolio.taxi.control.RunControlRecord;
import dev.portfolio.taxi.input.InputModels.SemanticTrip;
import dev.portfolio.taxi.metrics.AmplificationMetrics;
import dev.portfolio.taxi.metrics.StatefulOperatorMetrics;
import dev.portfolio.taxi.measurement.ResultTransport;
import dev.portfolio.taxi.model.Grid;
import dev.portfolio.taxi.ranking.RankingMetrics;
import dev.portfolio.taxi.ranking.VirtualShards;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class Query1Functions {
    private Query1Functions() {}

    public static final class RouteWindow
            extends KeyedProcessFunction<Long, RouteUpdate, RouteAggregate>
            implements CheckpointedFunction {
        private transient MapState<Long, Long> occurrences;
        private transient MapState<Long, ExpirationBucket> expirationBuckets;
        private transient MapState<Long, RouteInputBucket> pendingInputs;
        private transient ValueState<Long> count;
        private transient ValueState<Long> freshnessTime;
        private transient ValueState<Long> freshnessLine;
        private transient ValueState<String> runId;
        private transient ValueState<String> datasetId;
        private final StatefulOperatorMetrics stateMetrics = new StatefulOperatorMetrics();

        @Override
        public void open(OpenContext context) {
            occurrences = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("route-occurrences-v1", Long.class, Long.class));
            expirationBuckets = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(
                            "route-expiration-buckets-v1",
                            TypeInformation.of(Long.class),
                            TypeInformation.of(ExpirationBucket.class)));
            pendingInputs = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(
                            "route-pending-inputs-v1",
                            TypeInformation.of(Long.class),
                            TypeInformation.of(RouteInputBucket.class)));
            count = state("route-count-v1", Long.class);
            freshnessTime = state("route-freshness-time-v1", Long.class);
            freshnessLine = state("route-freshness-line-v1", Long.class);
            runId = state("route-run-id-v1", String.class);
            datasetId = state("route-dataset-id-v1", String.class);
            stateMetrics.open(getRuntimeContext(), "query1_route_window");
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            stateMetrics.initialize(context, "query1-route-state-metrics-v1");
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            stateMetrics.snapshot(context);
        }

        private <T> ValueState<T> state(String name, Class<T> type) {
            return getRuntimeContext().getState(new ValueStateDescriptor<>(name, type));
        }

        @Override
        public void processElement(RouteUpdate value, Context context, Collector<RouteAggregate> output)
                throws Exception {
            if (occurrences.contains(value.sourceLine())) {
                return;
            }
            RouteInputBucket bucket = pendingInputs.get(value.eventTimeMillis());
            if (bucket == null) {
                bucket = new RouteInputBucket();
                stateMetrics.rememberPendingBoundary(value.eventTimeMillis());
                stateMetrics.addRegisteredEventTimeTimers(1L);
            }
            for (RouteUpdate pending : bucket.values) {
                if (pending.sourceLine() == value.sourceLine()) {
                    return;
                }
            }
            bucket.values.add(value);
            long bufferedAt = stateMetrics.rememberBufferedEvent();
            bucket.bufferedAtEpochMillis.add(bufferedAt);
            pendingInputs.put(value.eventTimeMillis(), bucket);
            context.timerService().registerEventTimeTimer(value.eventTimeMillis());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<RouteAggregate> output)
                throws Exception {
            ExpirationBucket bucket = expirationBuckets.get(timestamp);
            if (bucket != null) {
                stateMetrics.addRegisteredEventTimeTimers(-1L);
                bucket.sourceLines.sort(Long::compareTo);
                for (long sourceLine : bucket.sourceLines) {
                    Long removedTime = occurrences.get(sourceLine);
                    if (removedTime == null) {
                        continue;
                    }
                    occurrences.remove(sourceLine);
                    stateMetrics.addActiveEntries(-1L);
                    long updatedCount = value(count) - 1L;
                    if (updatedCount < 0) {
                        throw new IllegalStateException("route count became negative");
                    }
                    if (updatedCount > 0L) {
                        count.update(updatedCount);
                    }
                    Long currentLine = freshnessLine.value();
                    if (currentLine != null && currentLine == sourceLine && updatedCount > 0) {
                        recomputeFreshness();
                    }
                    if (updatedCount == 0) {
                        freshnessTime.clear();
                        freshnessLine.clear();
                    }
                    output.collect(aggregate(
                            context.getCurrentKey(),
                            updatedCount,
                            timestamp,
                            "event_time_timer",
                            sourceLine,
                            null,
                            0L,
                            updatedCount > 0));
                    if (updatedCount == 0L) {
                        count.clear();
                        runId.clear();
                        datasetId.clear();
                    }
                }
                expirationBuckets.remove(timestamp);
            }

            RouteInputBucket inputs = pendingInputs.get(timestamp);
            if (inputs != null) {
                stateMetrics.forgetPendingBoundary(timestamp);
                stateMetrics.addRegisteredEventTimeTimers(-1L);
                List<Integer> order = new ArrayList<>(inputs.values.size());
                for (int index = 0; index < inputs.values.size(); index++) {
                    order.add(index);
                }
                order.sort(java.util.Comparator.comparingLong(index -> inputs.values.get(index).sourceLine()));
                for (int index : order) {
                    RouteUpdate input = inputs.values.get(index);
                    if (occurrences.contains(input.sourceLine())) {
                        stateMetrics.forgetBufferedEvent(inputs.bufferedAtEpochMillis.get(index));
                        continue;
                    }
                    occurrences.put(input.sourceLine(), input.eventTimeMillis());
                    stateMetrics.addActiveEntries(1L);
                    long updatedCount = value(count) + 1L;
                    count.update(updatedCount);
                    long expiry = input.eventTimeMillis() + RouteRanking.WINDOW_MILLIS;
                    ExpirationBucket expirations = expirationBuckets.get(expiry);
                    if (expirations == null) {
                        expirations = new ExpirationBucket();
                        stateMetrics.addRegisteredEventTimeTimers(1L);
                    }
                    expirations.sourceLines.add(input.sourceLine());
                    expirationBuckets.put(expiry, expirations);
                    context.timerService().registerEventTimeTimer(expiry);
                    Long currentTime = freshnessTime.value();
                    Long currentLine = freshnessLine.value();
                    if (currentTime == null
                            || input.eventTimeMillis() > currentTime
                            || (input.eventTimeMillis() == currentTime
                                    && input.sourceLine() > currentLine)) {
                        freshnessTime.update(input.eventTimeMillis());
                        freshnessLine.update(input.sourceLine());
                    }
                    runId.update(input.runId());
                    datasetId.update(input.datasetId());
                    output.collect(aggregate(
                            input.routeId(),
                            updatedCount,
                            timestamp,
                            "input",
                            input.sourceLine(),
                            input.eventId(),
                            input.producerSendEpochMicros(),
                            true));
                    stateMetrics.forgetBufferedEvent(inputs.bufferedAtEpochMillis.get(index));
                }
                pendingInputs.remove(timestamp);
            }
        }

        private RouteAggregate aggregate(
                long routeId,
                long currentCount,
                long effectiveTime,
                String triggerType,
                long triggerSourceLine,
                String triggerEventId,
                long producerSendMicros,
                boolean active)
                throws Exception {
            int pickup = Grid.routePickupCell(routeId);
            int dropoff = Grid.routeDropoffCell(routeId);
            return new RouteAggregate(
                    routeId,
                    pickup,
                    dropoff,
                    currentCount,
                    active ? freshnessTime.value() : 0L,
                    active ? freshnessLine.value() : 0L,
                    effectiveTime,
                    triggerType,
                    triggerSourceLine,
                    triggerEventId,
                    runId.value(),
                    datasetId.value(),
                    producerSendMicros,
                    active);
        }

        private void recomputeFreshness() throws Exception {
            long newestTime = Long.MIN_VALUE;
            long newestLine = Long.MIN_VALUE;
            for (var occurrence : occurrences.entries()) {
                long line = occurrence.getKey();
                long time = occurrence.getValue();
                if (time > newestTime || (time == newestTime && line > newestLine)) {
                    newestTime = time;
                    newestLine = line;
                }
            }
            freshnessTime.update(newestTime);
            freshnessLine.update(newestLine);
        }

        private static long value(ValueState<Long> state) throws IOException {
            Long result = state.value();
            return result == null ? 0L : result;
        }
    }

    public static final class ShardRank
            extends KeyedProcessFunction<Integer, RouteAggregate, GlobalMessage>
            implements CheckpointedFunction {
        private transient ValueState<RouteRanking> rankingState;
        private transient ValueState<String> signatureState;
        private transient MapState<Long, AggregateBucket> pending;
        private transient RankingMetrics rankingMetrics;
        private final StatefulOperatorMetrics stateMetrics = new StatefulOperatorMetrics();

        @Override
        public void open(OpenContext context) {
            rankingState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>(
                            "query1-shard-ranking-v1", TypeInformation.of(RouteRanking.class)));
            signatureState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("query1-shard-signature-v1", String.class));
            pending = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(
                            "query1-shard-pending-boundaries-v1",
                            TypeInformation.of(Long.class),
                            TypeInformation.of(AggregateBucket.class)));
            rankingMetrics = new RankingMetrics(getRuntimeContext(), "query1");
            stateMetrics.open(getRuntimeContext(), "query1_shard_rank");
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            stateMetrics.initialize(context, "query1-shard-state-metrics-v1");
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            stateMetrics.snapshot(context);
        }

        @Override
        public void processElement(RouteAggregate value, Context context, Collector<GlobalMessage> output)
                throws Exception {
            rankingMetrics.input(context.getCurrentKey());
            AggregateBucket bucket = pending.get(value.effectiveTimeMillis());
            if (bucket == null) {
                bucket = new AggregateBucket();
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
            AggregateBucket bucket = pending.get(timestamp);
            if (bucket == null) {
                return;
            }
            stateMetrics.forgetPendingBoundary(timestamp);
            stateMetrics.addRegisteredEventTimeTimers(-1L);
            RouteRanking ranking = rankingState.value();
            if (ranking == null) {
                ranking = new RouteRanking();
            }
            String runId = null;
            String datasetId = null;
            long producerSendMicros = 0L;
            for (RouteAggregate value : bucket.values) {
                if (value.active()) {
                    if (!ranking.contains(value.routeId())) {
                        stateMetrics.addActiveEntries(1L);
                    }
                    ranking.set(new RouteRankEntry(
                            value.routeId(),
                            value.pickupCell(),
                            value.dropoffCell(),
                            value.count(),
                            value.freshnessTimeMillis(),
                            value.freshnessSourceLine()));
                } else {
                    if (ranking.contains(value.routeId())) {
                        stateMetrics.addActiveEntries(-1L);
                    }
                    ranking.remove(value.routeId());
                }
                runId = value.runId();
                datasetId = value.datasetId();
                producerSendMicros = Math.max(producerSendMicros, value.inputProducerSendEpochMicros());
            }
            String signature = RouteRanking.signature(ranking.top());
            String previous = signatureState.value();
            if (!signature.equals(previous)) {
                output.collect(new ShardTopMessage(
                        context.getCurrentKey(),
                        ranking.top(),
                        timestamp,
                        runId,
                        datasetId,
                        producerSendMicros));
                rankingMetrics.snapshot();
                signatureState.update(signature);
            }
            rankingState.update(ranking);
            pending.remove(timestamp);
        }
    }

    public static final class GlobalRank
            extends KeyedProcessFunction<Integer, GlobalMessage, Query1Result>
            implements CheckpointedFunction {
        private transient MapState<Integer, List<RouteRankEntry>> shardTops;
        private transient MapState<Long, BoundaryFlags> boundaries;
        private transient MapState<Integer, Boolean> eosPartitions;
        private transient ValueState<String> lastSignature;
        private transient ValueState<Long> terminalBoundary;
        private transient ValueState<Boolean> terminalComplete;
        private transient Counter globalInputs;
        private transient Counter globalShardInputs;
        private transient Counter globalEosInputs;
        private transient Counter finalizedBoundaries;
        private long maximumBoundarySeen = Long.MIN_VALUE;
        private long currentGlobalWatermark = Long.MIN_VALUE;
        private long maximumBoundaryLeadMillis;
        private final StatefulOperatorMetrics stateMetrics = new StatefulOperatorMetrics();

        @Override
        public void open(OpenContext context) {
            shardTops = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(
                            "query1-global-shard-tops-v1",
                            TypeInformation.of(Integer.class),
                            TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<List<RouteRankEntry>>() {})));
            boundaries = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(
                            "query1-global-boundaries-v3",
                            TypeInformation.of(Long.class),
                            TypeInformation.of(BoundaryFlags.class)));
            eosPartitions = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("query1-global-eos-v1", Integer.class, Boolean.class));
            lastSignature = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("query1-global-signature-v1", String.class));
            terminalBoundary = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("query1-global-terminal-boundary-v1", Long.class));
            terminalComplete = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("query1-global-terminal-complete-v1", Boolean.class));
            globalInputs = AmplificationMetrics.counter(
                    getRuntimeContext(), "Q1_G_global_ranking_inputs");
            globalShardInputs = AmplificationMetrics.counter(
                    getRuntimeContext(), "Q1_G_shard_snapshots");
            globalEosInputs = AmplificationMetrics.counter(
                    getRuntimeContext(), "Q1_G_eos_messages");
            finalizedBoundaries = AmplificationMetrics.counter(
                    getRuntimeContext(), "Q1_H_finalized_boundaries");
            var metrics = getRuntimeContext().getMetricGroup().addGroup("query1");
            metrics.gauge("global_watermark", () -> currentGlobalWatermark);
            metrics.gauge("maximum_boundary_lead_ms", () -> maximumBoundaryLeadMillis);
            stateMetrics.open(getRuntimeContext(), "query1_global_rank");
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            stateMetrics.initialize(context, "query1-global-state-metrics-v3");
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            stateMetrics.snapshot(context);
        }

        @Override
        public void processElement(GlobalMessage value, Context context, Collector<Query1Result> output)
                throws Exception {
            globalInputs.inc();
            maximumBoundarySeen = Math.max(maximumBoundarySeen, value.effectiveTimeMillis());
            rememberGlobalWatermark(context.timerService().currentWatermark());
            Long terminal = terminalBoundary.value();
            if (Boolean.TRUE.equals(terminalComplete.value())
                    && terminal != null
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
                flags.inputProducerSendEpochMicros = Math.max(
                        flags.inputProducerSendEpochMicros,
                        shard.inputProducerSendEpochMicros());
                flags.rememberRequiredArrival(ResultTransport.nowEpochMicros());
            } else if (value instanceof BoundaryMessage boundary) {
                flags.input |= boundary.input();
                flags.timer |= boundary.timer();
                flags.rememberIdentity(boundary.runId(), boundary.datasetId());
                flags.inputProducerSendEpochMicros = Math.max(
                        flags.inputProducerSendEpochMicros,
                        boundary.inputProducerSendEpochMicros());
                flags.rememberTransport(boundary, ResultTransport.nowEpochMicros());
            } else if (value instanceof EosMessage eos) {
                globalEosInputs.inc();
                if (!eosPartitions.contains(eos.partition())) {
                    stateMetrics.addActiveEntries(1L);
                }
                eosPartitions.put(eos.partition(), true);
                flags.rememberIdentity(eos.runId(), eos.datasetId());
                int observed = 0;
                for (Boolean ignored : eosPartitions.values()) {
                    observed++;
                }
                if (observed >= eos.expectedPartitionCount()) {
                    flags.terminal = true;
                    terminalBoundary.update(eos.effectiveTimeMillis());
                }
            } else {
                throw new IllegalArgumentException("unknown global message");
            }
            boundaries.put(value.effectiveTimeMillis(), flags);
            context.timerService().registerEventTimeTimer(value.effectiveTimeMillis());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Query1Result> output)
                throws Exception {
            rememberGlobalWatermark(context.timerService().currentWatermark());
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
            if (flags == null) {
                return;
            }
            finalizedBoundaries.inc();
            long watermarkClosedEpochMicros = ResultTransport.nowEpochMicros();
            stateMetrics.forgetPendingBoundary(timestamp);
            stateMetrics.addRegisteredEventTimeTimers(-1L);
            for (var update : flags.shardUpdates.entrySet()) {
                List<RouteRankEntry> previousTop = shardTops.get(update.getKey());
                if (previousTop != null) {
                    stateMetrics.addActiveEntries(-previousTop.size());
                }
                shardTops.put(update.getKey(), update.getValue());
                stateMetrics.addActiveEntries(update.getValue().size());
            }
            List<List<RouteRankEntry>> candidates = new ArrayList<>();
            for (List<RouteRankEntry> shard : shardTops.values()) {
                if (!shard.isEmpty()) {
                    candidates.add(shard);
                }
            }
            long globalBoundaryStartedEpochMicros = ResultTransport.nowEpochMicros();
            List<RouteRankEntry> top = GlobalTopTenMerger.merge(candidates);
            String signature = RouteRanking.signature(top);
            String previous = lastSignature.value();
            if (flags.terminal || previous == null || !signature.equals(previous)) {
                String trigger = flags.input
                        ? (flags.timer ? "mixed" : "input")
                        : (flags.timer ? "event_time_timer" : "terminal");
                long resultEmittedEpochMicros = ResultTransport.nowEpochMicros();
                ResultTransport transport = flags.transport(
                        watermarkClosedEpochMicros,
                        globalBoundaryStartedEpochMicros,
                        resultEmittedEpochMicros);
                Query1Result result = new Query1Result(
                        timestamp,
                        trigger,
                        null,
                        null,
                        flags.terminal,
                        top,
                        flags.runId,
                        flags.datasetId,
                        transport);
                output.collect(result);
                if (flags.terminal) {
                    context.output(
                            CompletionSignals.QUERY1,
                            RunControlRecord.queryComplete(
                                    "query1",
                                    timestamp,
                                    flags.runId,
                                    flags.datasetId,
                                    result.outputId()));
                }
                lastSignature.update(signature);
            }
            if (flags.terminal) {
                terminalComplete.update(true);
            }
            boundaries.remove(timestamp);
        }

        private void rememberGlobalWatermark(long watermark) {
            currentGlobalWatermark = Math.max(currentGlobalWatermark, watermark);
            if (currentGlobalWatermark != Long.MIN_VALUE && maximumBoundarySeen != Long.MIN_VALUE) {
                maximumBoundaryLeadMillis = Math.max(
                        maximumBoundaryLeadMillis,
                        Math.max(0L, maximumBoundarySeen - currentGlobalWatermark));
            }
        }
    }

    public static RouteUpdate routeUpdate(SemanticTrip event) {
        OptionalInt pickup = Grid.query1Cell(event.pickupLongitude(), event.pickupLatitude());
        OptionalInt dropoff = Grid.query1Cell(event.dropoffLongitude(), event.dropoffLatitude());
        if (pickup.isEmpty() || dropoff.isEmpty()) {
            return null;
        }
        return new RouteUpdate(
                Grid.routeId(pickup.getAsInt(), dropoff.getAsInt()),
                pickup.getAsInt(),
                dropoff.getAsInt(),
                event.eventTimeMillis(),
                event.sourceLine(),
                event.eventId(),
                event.runId(),
                event.datasetId(),
                0L);
    }

    public static int rankingShard(long routeId, int shards) {
        return VirtualShards.shard(routeId, shards);
    }

    public static final class ExpirationBucket implements Serializable {
        private final List<Long> sourceLines = new ArrayList<>();
    }

    public static final class RouteInputBucket implements Serializable {
        private final List<RouteUpdate> values = new ArrayList<>();
        private final List<Long> bufferedAtEpochMillis = new ArrayList<>();
    }

    public static final class AggregateBucket implements Serializable {
        private final List<RouteAggregate> values = new ArrayList<>();
    }

    public static final class BoundaryFlags implements Serializable {
        private boolean input;
        private boolean timer;
        private boolean terminal;
        private String runId;
        private String datasetId;
        private long inputProducerSendEpochMicros;
        private long inputKafkaAppendEpochMicros;
        private long flinkSourceReadEpochMicros;
        private long boundaryFirstInputAppendEpochMicros = Long.MAX_VALUE;
        private int triggerInputPartition = -1;
        private long triggerInputSourceLine;
        private long triggerInputSourceReadEpochMicros;
        private long partitionBoundaryEmittedEpochMicros;
        private long firstInputBoundaryArrivalEpochMicros = Long.MAX_VALUE;
        private long lastInputBoundaryArrivalEpochMicros;
        private long lastRequiredGlobalMessageArrivalEpochMicros;
        private long maximumPartitionBoundaryDeliveryDurationMicros;
        private final java.util.Map<Integer, List<RouteRankEntry>> shardUpdates =
                new java.util.HashMap<>();

        private void rememberIdentity(String candidateRunId, String candidateDatasetId) {
            if (runId == null) {
                runId = candidateRunId;
                datasetId = candidateDatasetId;
            } else if (!runId.equals(candidateRunId) || !datasetId.equals(candidateDatasetId)) {
                throw new IllegalArgumentException("global Query 1 state mixed run or dataset identities");
            }
        }

        private void rememberTransport(BoundaryMessage boundary, long arrivalEpochMicros) {
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

        private void rememberRequiredArrival(long arrivalEpochMicros) {
            lastRequiredGlobalMessageArrivalEpochMicros = Math.max(
                    lastRequiredGlobalMessageArrivalEpochMicros, arrivalEpochMicros);
        }

        private ResultTransport transport(
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
