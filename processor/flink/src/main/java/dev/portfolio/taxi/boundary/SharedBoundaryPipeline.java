package dev.portfolio.taxi.boundary;

import static dev.portfolio.taxi.boundary.SharedBoundaryModels.BoundarySignal;
import static dev.portfolio.taxi.boundary.SharedBoundaryModels.Origin;
import static dev.portfolio.taxi.boundary.SharedBoundaryModels.SharedBoundary;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public final class SharedBoundaryPipeline {
    private SharedBoundaryPipeline() {}

    public static SingleOutputStreamOperator<SharedBoundary> build(
            DataStream<dev.portfolio.taxi.query1.Query1Models.GlobalMessage> inputBoundaries,
            DataStream<dev.portfolio.taxi.query1.Query1Models.GlobalMessage> query1Timers,
            DataStream<dev.portfolio.taxi.query2.Query2Models.GlobalMessage> query2Timers,
            int parallelism) {
        SingleOutputStreamOperator<BoundarySignal> inputs = inputBoundaries
                .map(value -> BoundarySignal.from(boundary(value), Origin.INPUT))
                .returns(TypeInformation.of(BoundarySignal.class))
                .name("Shared input boundary candidates")
                .uid("taxi-shared-input-boundary-candidates-v1")
                .setParallelism(parallelism)
                .slotSharingGroup("default");
        SingleOutputStreamOperator<BoundarySignal> query1 = query1Timers
                .map(value -> BoundarySignal.from(boundary(value), Origin.QUERY1_TIMER))
                .returns(TypeInformation.of(BoundarySignal.class))
                .name("Shared Query 1 timer candidates")
                .uid("taxi-shared-query1-timer-candidates-v1")
                .setParallelism(parallelism);
        SingleOutputStreamOperator<BoundarySignal> query2 = query2Timers
                .map(value -> BoundarySignal.from(boundary(value), Origin.QUERY2_TIMER))
                .returns(TypeInformation.of(BoundarySignal.class))
                .name("Shared Query 2 timer candidates")
                .uid("taxi-shared-query2-timer-candidates-v1")
                .setParallelism(parallelism);
        return inputs.union(query1, query2)
                .keyBy(BoundarySignal::effectiveTimeMillis)
                .process(new SharedBoundaryCoalescer())
                .returns(TypeInformation.of(SharedBoundary.class))
                .name("Consolidate shared event-time boundaries")
                .uid("taxi-shared-boundary-coalescer-v3")
                .setParallelism(parallelism);
    }

    public static SingleOutputStreamOperator<dev.portfolio.taxi.query1.Query1Models.GlobalMessage>
            forQuery1(DataStream<SharedBoundary> boundaries, int parallelism) {
        return boundaries
                .map(value -> (dev.portfolio.taxi.query1.Query1Models.GlobalMessage)
                        dev.portfolio.taxi.query1.Query1Models.BoundaryMessage.from(value))
                .returns(TypeInformation.of(
                        dev.portfolio.taxi.query1.Query1Models.GlobalMessage.class))
                .name("Share consolidated boundaries with Query 1")
                .uid("taxi-shared-boundaries-for-query1-v1")
                .setParallelism(parallelism);
    }

    public static SingleOutputStreamOperator<dev.portfolio.taxi.query2.Query2Models.GlobalMessage>
            forQuery2(DataStream<SharedBoundary> boundaries, int parallelism) {
        return boundaries
                .map(value -> (dev.portfolio.taxi.query2.Query2Models.GlobalMessage)
                        dev.portfolio.taxi.query2.Query2Models.BoundaryMessage.from(value))
                .returns(TypeInformation.of(
                        dev.portfolio.taxi.query2.Query2Models.GlobalMessage.class))
                .name("Share consolidated boundaries with Query 2")
                .uid("taxi-shared-boundaries-for-query2-v1")
                .setParallelism(parallelism);
    }

    private static BoundaryFields boundary(Object value) {
        if (value instanceof BoundaryFields fields) {
            return fields;
        }
        throw new IllegalArgumentException("shared boundary stream contained a non-boundary message");
    }
}
