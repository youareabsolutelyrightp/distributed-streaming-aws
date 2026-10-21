package dev.portfolio.taxi.query1;

import static dev.portfolio.taxi.query1.Query1Models.BoundaryMessage;
import static dev.portfolio.taxi.query1.Query1Models.GlobalMessage;
import static dev.portfolio.taxi.input.InputModels.InputEnvelope;
import static dev.portfolio.taxi.input.InputModels.SemanticTrip;
import static dev.portfolio.taxi.input.InputModels.TripEvent;
import static dev.portfolio.taxi.query1.Query1Models.RouteAggregate;
import static dev.portfolio.taxi.query1.Query1Models.RouteUpdate;

import dev.portfolio.taxi.boundary.PartitionBoundaryInput;
import dev.portfolio.taxi.boundary.PartitionBoundaryPreAggregator;
import dev.portfolio.taxi.config.JobSettings;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public final class Query1Pipeline {
    private Query1Pipeline() {}

    public static DistributedComponents distributedComponents(
            DataStream<InputEnvelope> input, JobSettings job, Query1Settings settings) {
        return distributedComponents(input, job, settings, "default");
    }

    public static DistributedComponents distributedComponents(
            DataStream<InputEnvelope> input,
            JobSettings job,
            Query1Settings settings,
            String boundarySlotSharingGroup) {
        SingleOutputStreamOperator<TripEvent> trips = input
                .filter(value -> value instanceof TripEvent)
                .slotSharingGroup("default")
                .map(value -> (TripEvent) value)
                .returns(TypeInformation.of(TripEvent.class))
                .name("Query 1 trip envelopes")
                .uid("query1-trip-envelopes-v1")
                .setParallelism(job.sourceParallelism())
                .slotSharingGroup("default");

        SingleOutputStreamOperator<SemanticTrip> semanticTrips = trips
                .map(TripEvent::semantic)
                .returns(TypeInformation.of(SemanticTrip.class))
                .name("Compact Query 1 semantic trips")
                .uid("query1-semantic-trips-v1")
                .setParallelism(job.sourceParallelism());

        SingleOutputStreamOperator<RouteUpdate> updates = semanticTrips
                .flatMap((SemanticTrip trip, org.apache.flink.util.Collector<RouteUpdate> output) -> {
                    RouteUpdate update = Query1Functions.routeUpdate(trip);
                    if (update != null) {
                        output.collect(update);
                    }
                })
                .returns(TypeInformation.of(RouteUpdate.class))
                .name("Map valid Query 1 routes")
                .uid("query1-valid-route-map-v1")
                .setParallelism(job.sourceParallelism());

        SingleOutputStreamOperator<RouteAggregate> aggregates = updates
                .keyBy(RouteUpdate::routeId)
                .process(new Query1Functions.RouteWindow())
                .name("Exact 30-minute route state")
                .uid("query1-route-window-v1")
                .setParallelism(settings.routeParallelism());

        SingleOutputStreamOperator<GlobalMessage> shardMessages = aggregates
                .keyBy(value -> Query1Functions.rankingShard(value.routeId(), settings.rankingShards()))
                .process(new Query1Functions.ShardRank())
                .returns(TypeInformation.of(GlobalMessage.class))
                .name("Query 1 shard-local top ten")
                .uid("query1-shard-top-v3")
                .setParallelism(settings.rankingParallelism())
                .setMaxParallelism(job.rankingMaxParallelism());
        SingleOutputStreamOperator<PartitionBoundaryInput> compactInputBoundaries = input
                .map(value -> PartitionBoundaryInput.fromEnvelope(value, job.inputPartitions()))
                .returns(TypeInformation.of(PartitionBoundaryInput.class))
                .name("Compact input boundary records")
                .uid("query1-compact-input-boundaries-v1")
                .setParallelism(job.sourceParallelism())
                .slotSharingGroup("source");

        SingleOutputStreamOperator<GlobalMessage> inputBoundaries = compactInputBoundaries
                .keyBy(PartitionBoundaryInput::stablePartitionKey)
                .process(new PartitionBoundaryPreAggregator(job.inputPartitions()))
                .returns(TypeInformation.of(GlobalMessage.class))
                .name("Pre-aggregate input boundaries by Kafka partition")
                .uid("query1-input-boundaries-v5")
                .setParallelism(job.sourceParallelism())
                .setMaxParallelism(128)
                .slotSharingGroup(boundarySlotSharingGroup);

        DataStream<GlobalMessage> eosMessages =
                inputBoundaries.getSideOutput(PartitionBoundaryPreAggregator.EOS_MESSAGES);
        SingleOutputStreamOperator<GlobalMessage> timerBoundaries = aggregates
                .filter(value -> value.triggerType().equals("event_time_timer"))
                .map(value -> (GlobalMessage) new BoundaryMessage(
                        value.effectiveTimeMillis(),
                        false,
                        true,
                        value.runId(),
                        value.datasetId(),
                        0L,
                        0L,
                        0L))
                .returns(TypeInformation.of(GlobalMessage.class))
                .name("Query 1 timer boundary signals")
                .uid("query1-timer-boundaries-v1")
                .setParallelism(settings.routeParallelism());

        return new DistributedComponents(
                shardMessages,
                inputBoundaries,
                timerBoundaries,
                eosMessages);
    }

    public static SingleOutputStreamOperator<Query1Result> globalWithSharedBoundaries(
            DistributedComponents components,
            DataStream<GlobalMessage> sharedBoundaries) {
        return components.shardMessages()
                .union(sharedBoundaries, components.eosMessages())
                .keyBy(ignored -> 0)
                .process(new Query1Functions.GlobalRank())
                .name("Query 1 exact global top ten")
                .uid("query1-global-top-v3")
                .setParallelism(1)
                .slotSharingGroup("query1-global");
    }

    public record DistributedComponents(
            DataStream<GlobalMessage> shardMessages,
            DataStream<GlobalMessage> inputBoundaries,
            DataStream<GlobalMessage> timerBoundaries,
            DataStream<GlobalMessage> eosMessages) {}

}
