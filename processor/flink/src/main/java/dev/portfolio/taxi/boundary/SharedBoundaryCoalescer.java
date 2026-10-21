package dev.portfolio.taxi.boundary;

import static dev.portfolio.taxi.boundary.SharedBoundaryModels.BoundarySignal;
import static dev.portfolio.taxi.boundary.SharedBoundaryModels.Origin;
import static dev.portfolio.taxi.boundary.SharedBoundaryModels.SharedBoundary;

import dev.portfolio.taxi.metrics.AmplificationMetrics;
import dev.portfolio.taxi.metrics.StatefulOperatorMetrics;
import dev.portfolio.taxi.measurement.ResultTransport;
import java.io.Serializable;
import java.util.TreeMap;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class SharedBoundaryCoalescer
        extends KeyedProcessFunction<Long, BoundarySignal, SharedBoundary>
        implements CheckpointedFunction {
    private transient ValueState<Accumulator> accumulator;
    private transient Counter finalizedBoundaries;
    private transient Counter inputCandidates;
    private transient Counter query1TimerCandidates;
    private transient Counter query2TimerCandidates;
    private final StatefulOperatorMetrics stateMetrics = new StatefulOperatorMetrics();

    @Override
    public void open(OpenContext context) {
        accumulator = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "shared-boundary-accumulator-v3", Accumulator.class));
        finalizedBoundaries = AmplificationMetrics.counter(
                getRuntimeContext(), "shared_boundaries");
        inputCandidates = AmplificationMetrics.counter(
                getRuntimeContext(), "shared_input_candidates");
        query1TimerCandidates = AmplificationMetrics.counter(
                getRuntimeContext(), "shared_query1_timer_candidates");
        query2TimerCandidates = AmplificationMetrics.counter(
                getRuntimeContext(), "shared_query2_timer_candidates");
        stateMetrics.open(getRuntimeContext(), "shared_boundary_coalescer");
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        stateMetrics.initialize(context, "shared-boundary-state-metrics-v3");
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        stateMetrics.snapshot(context);
    }

    @Override
    public void processElement(
            BoundarySignal value, Context context, Collector<SharedBoundary> output)
            throws Exception {
        Accumulator current = accumulator.value();
        if (value.origin() == Origin.INPUT) inputCandidates.inc();
        else if (value.origin() == Origin.QUERY1_TIMER) query1TimerCandidates.inc();
        else query2TimerCandidates.inc();
        if (current == null) {
            current = new Accumulator();
            stateMetrics.addActiveEntries(1L);
            stateMetrics.rememberPendingBoundary(value.effectiveTimeMillis());
            stateMetrics.addRegisteredEventTimeTimers(1L);
            context.timerService().registerEventTimeTimer(value.effectiveTimeMillis());
        }
        current.accept(value);
        current.bufferedArrivals.merge(stateMetrics.rememberBufferedEvent(), 1, Integer::sum);
        accumulator.update(current);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<SharedBoundary> output)
            throws Exception {
        Accumulator current = accumulator.value();
        if (current == null) {
            return;
        }
        output.collect(current.toBoundary(timestamp, ResultTransport.nowEpochMicros()));
        finalizedBoundaries.inc();
        for (var entry : current.bufferedArrivals.entrySet()) {
            stateMetrics.forgetBufferedEvents(entry.getKey(), entry.getValue());
        }
        stateMetrics.addActiveEntries(-1L);
        stateMetrics.forgetPendingBoundary(timestamp);
        stateMetrics.addRegisteredEventTimeTimers(-1L);
        accumulator.clear();
    }

    public static final class Accumulator implements Serializable {
        private boolean input;
        private boolean timer;
        private String runId;
        private String datasetId;
        private long producerSendEpochMicros;
        private long boundaryFirstInputAppendEpochMicros = Long.MAX_VALUE;
        private long triggerInputAppendEpochMicros;
        private int triggerInputPartition = -1;
        private long triggerInputSourceLine;
        private long triggerInputSourceReadEpochMicros;
        private long flinkSourceReadEpochMicros;
        private long partitionBoundaryEmittedEpochMicros;
        private long boundarySignalArrivalEpochMicros;
        private final TreeMap<Long, Integer> bufferedArrivals = new TreeMap<>();

        public Accumulator() {}

        private void accept(BoundarySignal value) {
            input |= value.origin() == Origin.INPUT;
            timer |= value.origin() != Origin.INPUT;
            rememberIdentity(value.runId(), value.datasetId());
            producerSendEpochMicros = Math.max(
                    producerSendEpochMicros, value.producerSendEpochMicros());
            if (value.origin() == Origin.INPUT) {
                long firstAppend = value.boundaryFirstInputAppendEpochMicros() > 0L
                        ? value.boundaryFirstInputAppendEpochMicros()
                        : value.inputKafkaAppendEpochMicros();
                if (firstAppend > 0L) {
                    boundaryFirstInputAppendEpochMicros = Math.min(
                            boundaryFirstInputAppendEpochMicros, firstAppend);
                }
                if (laterTrigger(value)) {
                    triggerInputAppendEpochMicros = value.inputKafkaAppendEpochMicros();
                    triggerInputPartition = value.triggerInputPartition();
                    triggerInputSourceLine = value.triggerInputSourceLine();
                    triggerInputSourceReadEpochMicros =
                            value.triggerInputSourceReadEpochMicros() > 0L
                                    ? value.triggerInputSourceReadEpochMicros()
                                    : value.flinkSourceReadEpochMicros();
                }
            }
            flinkSourceReadEpochMicros = Math.max(
                    flinkSourceReadEpochMicros, value.flinkSourceReadEpochMicros());
            partitionBoundaryEmittedEpochMicros = Math.max(
                    partitionBoundaryEmittedEpochMicros,
                    value.partitionBoundaryEmittedEpochMicros());
            boundarySignalArrivalEpochMicros = Math.max(
                    boundarySignalArrivalEpochMicros,
                    value.boundarySignalArrivalEpochMicros());
        }

        private boolean laterTrigger(BoundarySignal value) {
            return value.inputKafkaAppendEpochMicros() > triggerInputAppendEpochMicros
                    || (value.inputKafkaAppendEpochMicros() == triggerInputAppendEpochMicros
                            && value.triggerInputSourceLine() > triggerInputSourceLine);
        }

        private SharedBoundary toBoundary(long timestamp, long sharedBoundaryEmittedEpochMicros) {
            return new SharedBoundary(
                    timestamp,
                    input,
                    timer,
                    runId,
                    datasetId,
                    producerSendEpochMicros,
                    triggerInputAppendEpochMicros,
                    flinkSourceReadEpochMicros,
                    boundaryFirstInputAppendEpochMicros == Long.MAX_VALUE
                            ? 0L
                            : boundaryFirstInputAppendEpochMicros,
                    triggerInputPartition,
                    triggerInputSourceLine,
                    triggerInputSourceReadEpochMicros,
                    partitionBoundaryEmittedEpochMicros,
                    sharedBoundaryEmittedEpochMicros,
                    boundarySignalArrivalEpochMicros);
        }

        private void rememberIdentity(String candidateRunId, String candidateDatasetId) {
            if (candidateRunId == null) {
                return;
            }
            if (runId == null) {
                runId = candidateRunId;
                datasetId = candidateDatasetId;
                return;
            }
            if (!runId.equals(candidateRunId) || !datasetId.equals(candidateDatasetId)) {
                throw new IllegalArgumentException("shared boundary mixed run or dataset identities");
            }
        }
    }
}
