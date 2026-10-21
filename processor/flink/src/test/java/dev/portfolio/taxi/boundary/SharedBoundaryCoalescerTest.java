package dev.portfolio.taxi.boundary;

import static dev.portfolio.taxi.boundary.SharedBoundaryModels.BoundarySignal;
import static dev.portfolio.taxi.boundary.SharedBoundaryModels.Origin;
import static dev.portfolio.taxi.boundary.SharedBoundaryModels.SharedBoundary;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

final class SharedBoundaryCoalescerTest {
    @Test
    void consolidatesInputAndBothTimerOriginsAtOneClosedBoundary() throws Exception {
        List<BoundarySignal> input = List.of(
                signal(1_000L, Origin.INPUT, 10L, 20L, 30L),
                signal(1_000L, Origin.INPUT, 40L, 15L, 25L),
                signal(1_000L, Origin.QUERY1_TIMER, 0L, 0L, 0L),
                signal(1_000L, Origin.QUERY2_TIMER, 0L, 0L, 0L),
                signal(2_000L, Origin.INPUT, 50L, 60L, 70L));

        Configuration configuration = new Configuration();
        configuration.set(TaskManagerOptions.NUM_TASK_SLOTS, 1);
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);
        environment.setParallelism(1);
        environment.getConfig().setAutoWatermarkInterval(5L);
        var source = environment
                .fromCollection(input, TypeInformation.of(BoundarySignal.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<BoundarySignal>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<BoundarySignal>)
                                (value, ignored) -> value.effectiveTimeMillis()));

        List<SharedBoundary> output = new ArrayList<>();
        try (CloseableIterator<SharedBoundary> iterator = source
                .keyBy(BoundarySignal::effectiveTimeMillis)
                .process(new SharedBoundaryCoalescer())
                .executeAndCollect()) {
            iterator.forEachRemaining(output::add);
        }

        assertEquals(2, output.size());
        SharedBoundary first = output.get(0);
        assertEquals(1_000L, first.effectiveTimeMillis());
        assertTrue(first.input());
        assertTrue(first.timer());
        assertEquals(40L, first.producerSendEpochMicros());
        assertEquals(20L, first.inputKafkaAppendEpochMicros());
        assertEquals(30L, first.flinkSourceReadEpochMicros());
        assertEquals(15L, first.boundaryFirstInputAppendEpochMicros());
        assertEquals(0L, first.boundarySignalArrivalEpochMicros());
    }

    private static BoundarySignal signal(
            long time, Origin origin, long send, long append, long read) {
        return new BoundarySignal(
                time, origin, "run", "dataset", send, append, read);
    }
}
