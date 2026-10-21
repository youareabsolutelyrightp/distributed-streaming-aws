package dev.portfolio.taxi.boundary;

import static dev.portfolio.taxi.query1.Query1Models.BoundaryMessage;
import static dev.portfolio.taxi.query1.Query1Models.EosMessage;
import static dev.portfolio.taxi.query1.Query1Models.GlobalMessage;

import dev.portfolio.taxi.measurement.ResultTransport;
import dev.portfolio.taxi.metrics.AmplificationMetrics;
import dev.portfolio.taxi.query1.Query1Models.BoundaryMessage;
import dev.portfolio.taxi.query1.Query1Models.EosMessage;

import java.io.Serializable;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public final class PartitionBoundaryPreAggregator
        extends KeyedProcessFunction<String, PartitionBoundaryInput, GlobalMessage> {
    public static final OutputTag<GlobalMessage> EOS_MESSAGES = new OutputTag<>(
            "partition-boundary-eos-v1", TypeInformation.of(GlobalMessage.class));

    private final int inputPartitions;
    private transient ValueState<Accumulator> pending;
    private transient Counter inputTrips;
    private transient Counter emittedSummaries;

    public PartitionBoundaryPreAggregator(int inputPartitions) {
        if (inputPartitions <= 0) throw new IllegalArgumentException("input partitions must be positive");
        this.inputPartitions = inputPartitions;
    }

    @Override
    public void open(OpenContext context) {
        pending = getRuntimeContext().getState(
                new ValueStateDescriptor<>("partition-boundary-pre-aggregation-v2", Accumulator.class));
        inputTrips = AmplificationMetrics.counter(getRuntimeContext(), "trace_input_trips");
        emittedSummaries = AmplificationMetrics.counter(getRuntimeContext(), "trace_partition_boundaries");
    }

    @Override
    public void processElement(PartitionBoundaryInput value, Context context, Collector<GlobalMessage> output)
            throws Exception {
        int partition = PartitionBoundaryInput.partitionFromKey(context.getCurrentKey());
        if (value.trip()) {
            inputTrips.inc();
            Accumulator current = pending.value();
            if (current != null && value.eventTimeMillis() < current.effectiveTimeMillis) {
                throw new IllegalArgumentException("Kafka partition event time moved backward");
            }
            if (current != null && value.eventTimeMillis() > current.effectiveTimeMillis) {
                emit(current, partition, output);
                current = null;
            }
            if (current == null) {
                current = new Accumulator(value.eventTimeMillis(), value.runId(), value.datasetId());
                context.timerService().registerEventTimeTimer(value.eventTimeMillis());
            }
            current.accept(value);
            pending.update(current);
            return;
        }
        if (value.eos()) {
            Accumulator current = pending.value();
            if (current != null) emit(current, partition, output);
            pending.clear();
            context.output(EOS_MESSAGES, new EosMessage(
                    value.eventTimeMillis(), value.kafkaPartition(), value.expectedPartitionCount(),
                    value.runId(), value.datasetId()));
            return;
        }
        throw new IllegalArgumentException("unsupported input boundary envelope");
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<GlobalMessage> output)
            throws Exception {
        Accumulator current = pending.value();
        if (current != null && current.effectiveTimeMillis <= timestamp) {
            emit(current, PartitionBoundaryInput.partitionFromKey(context.getCurrentKey()), output);
            pending.clear();
        }
    }

    private void emit(Accumulator current, int partition, Collector<GlobalMessage> output) {
        output.collect(current.toBoundary(partition));
        emittedSummaries.inc();
    }

    public static final class Accumulator implements Serializable {
        long effectiveTimeMillis;
        String runId;
        String datasetId;
        long producerSendEpochMicros;
        long firstAppendEpochMicros = Long.MAX_VALUE;
        long triggerAppendEpochMicros;
        long triggerSourceLine;
        long triggerSourceReadEpochMicros;
        long maximumSourceReadEpochMicros;

        public Accumulator() {}

        Accumulator(long effectiveTimeMillis, String runId, String datasetId) {
            this.effectiveTimeMillis = effectiveTimeMillis;
            this.runId = runId;
            this.datasetId = datasetId;
        }

        void accept(PartitionBoundaryInput trip) {
            if (!runId.equals(trip.runId()) || !datasetId.equals(trip.datasetId())) {
                throw new IllegalArgumentException("one input boundary mixed run or dataset identities");
            }
            if (trip.inputKafkaAppendEpochMicros() > triggerAppendEpochMicros
                    || (trip.inputKafkaAppendEpochMicros() == triggerAppendEpochMicros
                            && trip.sourceLine() > triggerSourceLine)) {
                triggerAppendEpochMicros = trip.inputKafkaAppendEpochMicros();
                triggerSourceLine = trip.sourceLine();
                triggerSourceReadEpochMicros = trip.flinkSourceReadEpochMicros();
            }
            producerSendEpochMicros = Math.max(producerSendEpochMicros, trip.producerSendEpochMicros());
            firstAppendEpochMicros = Math.min(firstAppendEpochMicros, trip.inputKafkaAppendEpochMicros());
            maximumSourceReadEpochMicros = Math.max(maximumSourceReadEpochMicros, trip.flinkSourceReadEpochMicros());
        }

        BoundaryMessage toBoundary(int partition) {
            return toBoundary(partition, ResultTransport.nowEpochMicros());
        }

        BoundaryMessage toBoundary(int partition, long emittedEpochMicros) {
            long causalEmissionEpochMicros = Math.max(emittedEpochMicros, maximumSourceReadEpochMicros);
            return new BoundaryMessage(
                    effectiveTimeMillis, true, false, runId, datasetId,
                    producerSendEpochMicros,
                    triggerAppendEpochMicros,
                    maximumSourceReadEpochMicros,
                    firstAppendEpochMicros != Long.MAX_VALUE ? firstAppendEpochMicros : 0L,
                    partition,
                    triggerSourceLine,
                    triggerSourceReadEpochMicros,
                    causalEmissionEpochMicros,
                    0L,
                    0L);
        }
    }
}
