package dev.portfolio.taxi.input;

import static dev.portfolio.taxi.input.InputModels.EosEvent;
import static dev.portfolio.taxi.input.InputModels.TripEvent;

import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

public final class InputReplayDeduplicator
        extends RichFlatMapFunction<KafkaInputRecord, KafkaInputRecord>
        implements CheckpointedFunction {
    private final int inputPartitions;
    private transient ListState<PartitionSnapshot> checkpointedPartitions;
    private transient PartitionState[] partitionStates;
    private transient PartitionMetrics[] metrics;
    private transient Map<String, Integer> expectedPartitions;

    public InputReplayDeduplicator(int inputPartitions) {
        if (inputPartitions <= 0) {
            throw new IllegalArgumentException("input partitions must be positive");
        }
        this.inputPartitions = inputPartitions;
    }

    @Override
    public void open(OpenContext context) {
        if (partitionStates == null) partitionStates = new PartitionState[inputPartitions];
        metrics = new PartitionMetrics[inputPartitions];
        expectedPartitions = new HashMap<>();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        checkpointedPartitions = context.getOperatorStateStore().getUnionListState(
                new ListStateDescriptor<>(
                        "input-replay-dedup-partitions-v2", PartitionSnapshot.class));
        partitionStates = new PartitionState[inputPartitions];
        for (PartitionSnapshot snapshot : checkpointedPartitions.get()) {
            PartitionState existing = partitionStates[snapshot.partition()];
            partitionStates[snapshot.partition()] = existing == null
                    ? snapshot.state()
                    : PartitionState.merge(existing, snapshot.state());
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedPartitions.clear();
        for (int partition = 0; partition < partitionStates.length; partition++) {
            if (partitionStates[partition] != null) {
                checkpointedPartitions.add(new PartitionSnapshot(partition, partitionStates[partition]));
            }
        }
    }

    @Override
    public void flatMap(KafkaInputRecord record, Collector<KafkaInputRecord> output)
            throws Exception {
        if (record.kafkaPartition() < 0 || record.kafkaPartition() >= inputPartitions) {
            throw new IllegalStateException("deduplication received an invalid Kafka partition");
        }
        if (!record.valid()) {
            output.collect(record);
            return;
        }
        if (record.envelope() instanceof EosEvent eos) {
            PartitionState state = partitionStates[record.kafkaPartition()];
            if (state != null) {
                state.requireIdentity(eos.runId(), eos.datasetId());
            }
            output.collect(record);
            return;
        }
        if (!(record.envelope() instanceof TripEvent trip)) {
            throw new IllegalArgumentException("unsupported input envelope for deduplication");
        }
        validateEventIdentity(trip);
        int expectedPartition = inputPartitions == 1
                ? 0
                : expectedPartitions.computeIfAbsent(
                        trip.medallion(),
                        medallion -> InputPartitioning.partition(medallion, inputPartitions));
        validatePartition(record.kafkaPartition(), expectedPartition, trip.sourceLine());
        PartitionState state = partitionStates[record.kafkaPartition()];
        if (state == null) {
            state = new PartitionState();
            partitionStates[record.kafkaPartition()] = state;
        }
        Decision decision = state.apply(trip);
        PartitionMetrics partitionMetrics = metrics[record.kafkaPartition()];
        if (partitionMetrics == null) {
            partitionMetrics = registerMetrics(record.kafkaPartition(), state);
            metrics[record.kafkaPartition()] = partitionMetrics;
        }
        if (decision == Decision.ACCEPT) {
            state.acceptedCount++;
            output.collect(record);
            return;
        }
        state.duplicateCount++;
        state.lowestDuplicateSourceLine = Math.min(
                state.lowestDuplicateSourceLine, trip.sourceLine());
        state.highestDuplicateSourceLine = Math.max(
                state.highestDuplicateSourceLine, trip.sourceLine());
        partitionMetrics.duplicates.inc();
    }

    static void validateTrip(int actualPartition, int inputPartitions, TripEvent trip) {
        validateEventIdentity(trip);
        int expectedPartition = inputPartitions == 1
                ? 0
                : InputPartitioning.partition(trip.medallion(), inputPartitions);
        validatePartition(actualPartition, expectedPartition, trip.sourceLine());
    }

    private static void validateEventIdentity(TripEvent trip) {
        if (trip.sourceLine() <= 0L) {
            throw new IllegalArgumentException("source_line_number must be positive");
        }
        if (!InputPartitioning.eventIdMatches(
                trip.datasetId(), trip.joinedRecordId(), trip.eventId())) {
            throw new IllegalArgumentException(
                    "event_id does not match dataset_id and joined_record_id at source line "
                            + trip.sourceLine());
        }
    }

    private static void validatePartition(
            int actualPartition, int expectedPartition, long sourceLine) {
        if (actualPartition != expectedPartition) {
            throw new IllegalArgumentException(
                    "source line " + sourceLine + " used Kafka partition "
                            + actualPartition + ", expected " + expectedPartition);
        }
    }

    private PartitionMetrics registerMetrics(int partition, PartitionState state) {
        MetricGroup group = getRuntimeContext()
                .getMetricGroup()
                .addGroup("taxi_input_dedup")
                .addGroup("partition", Integer.toString(partition));
        Counter duplicates = group.counter("replay_duplicates_total");
        group.gauge("highest_accepted_source_line", () -> state.highestAcceptedSourceLine);
        group.gauge("accepted_records", () -> state.acceptedCount);
        group.gauge("checkpointed_replay_duplicates", () -> state.duplicateCount);
        group.gauge("lowest_duplicate_source_line", () -> state.lowestDuplicateSourceLine);
        group.gauge("highest_duplicate_source_line", () -> state.highestDuplicateSourceLine);
        return new PartitionMetrics(duplicates);
    }

    enum Decision { ACCEPT, DUPLICATE }

    public static final class PartitionState implements Serializable {
        public long highestAcceptedSourceLine;
        public String eventIdAtHighestSourceLine;
        public String runId;
        public String datasetId;
        public long duplicateCount;
        public long acceptedCount;
        public long lowestDuplicateSourceLine = Long.MAX_VALUE;
        public long highestDuplicateSourceLine;

        public PartitionState() {}

        Decision apply(TripEvent trip) {
            if (highestAcceptedSourceLine == 0L) {
                remember(trip);
                return Decision.ACCEPT;
            }
            requireIdentity(trip.runId(), trip.datasetId());
            if (trip.sourceLine() > highestAcceptedSourceLine) {
                remember(trip);
                return Decision.ACCEPT;
            }
            if (trip.sourceLine() == highestAcceptedSourceLine
                    && !trip.eventId().equals(eventIdAtHighestSourceLine)) {
                throw new IllegalArgumentException(
                        "one source line has conflicting event IDs: " + trip.sourceLine());
            }
            return Decision.DUPLICATE;
        }

        void requireIdentity(String candidateRunId, String candidateDatasetId) {
            if (!runId.equals(candidateRunId) || !datasetId.equals(candidateDatasetId)) {
                throw new IllegalArgumentException(
                        "one Kafka partition mixed run or dataset identities");
            }
        }

        private void remember(TripEvent trip) {
            highestAcceptedSourceLine = trip.sourceLine();
            eventIdAtHighestSourceLine = trip.eventId();
            runId = trip.runId();
            datasetId = trip.datasetId();
        }

        static PartitionState merge(PartitionState left, PartitionState right) {
            if (left.highestAcceptedSourceLine == 0L) return right;
            if (right.highestAcceptedSourceLine == 0L) return left;
            left.requireIdentity(right.runId, right.datasetId);
            PartitionState selected = left.highestAcceptedSourceLine >= right.highestAcceptedSourceLine
                    ? left
                    : right;
            if (left.highestAcceptedSourceLine == right.highestAcceptedSourceLine
                    && !left.eventIdAtHighestSourceLine.equals(right.eventIdAtHighestSourceLine)) {
                throw new IllegalArgumentException(
                        "restored input replay state has conflicting event IDs");
            }
            selected.duplicateCount = Math.max(left.duplicateCount, right.duplicateCount);
            selected.acceptedCount = Math.max(left.acceptedCount, right.acceptedCount);
            selected.lowestDuplicateSourceLine = Math.min(
                    left.lowestDuplicateSourceLine, right.lowestDuplicateSourceLine);
            selected.highestDuplicateSourceLine = Math.max(
                    left.highestDuplicateSourceLine, right.highestDuplicateSourceLine);
            return selected;
        }
    }

    public record PartitionSnapshot(int partition, PartitionState state)
            implements Serializable {}

    private record PartitionMetrics(Counter duplicates) {}
}
