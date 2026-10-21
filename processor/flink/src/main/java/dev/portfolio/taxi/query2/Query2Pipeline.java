package dev.portfolio.taxi.query2;

import static dev.portfolio.taxi.input.InputModels.EosEvent;
import static dev.portfolio.taxi.input.InputModels.InputEnvelope;
import static dev.portfolio.taxi.input.InputModels.SemanticTrip;
import static dev.portfolio.taxi.input.InputModels.TripEvent;
import static dev.portfolio.taxi.query2.Query2Models.BoundaryMessage;
import static dev.portfolio.taxi.query2.Query2Models.CellDelta;
import static dev.portfolio.taxi.query2.Query2Models.EosMessage;
import static dev.portfolio.taxi.query2.Query2Models.FareInput;
import static dev.portfolio.taxi.query2.Query2Models.GlobalMessage;
import static dev.portfolio.taxi.query2.Query2Models.ProfitAggregate;
import static dev.portfolio.taxi.query2.Query2Models.ProfitComponentUpdate;

import java.util.Objects;

import dev.portfolio.taxi.config.JobSettings;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public final class Query2Pipeline {
    private Query2Pipeline() {}

    public static DistributedComponents distributedComponents(
            DataStream<InputEnvelope> input, JobSettings job, Query2Settings settings) {
        SingleOutputStreamOperator<TripEvent> trips = input
                .filter(value -> value instanceof TripEvent)
                .slotSharingGroup("default")
                .map(value -> (TripEvent) value)
                .returns(TypeInformation.of(TripEvent.class))
                .name("Query 2 trip envelopes")
                .uid("query2-trip-envelopes-v1")
                .setParallelism(job.sourceParallelism())
                .slotSharingGroup("default");

        SingleOutputStreamOperator<EosEvent> eos = input
                .filter(value -> value instanceof EosEvent)
                .slotSharingGroup("default")
                .map(value -> (EosEvent) value)
                .returns(TypeInformation.of(EosEvent.class))
                .name("Query 2 EOS envelopes")
                .uid("query2-eos-envelopes-v1")
                .setParallelism(job.sourceParallelism())
                .slotSharingGroup("default");

        SingleOutputStreamOperator<SemanticTrip> semanticTrips = trips
                .map(TripEvent::semantic)
                .returns(TypeInformation.of(SemanticTrip.class))
                .name("Compact Query 2 semantic trips")
                .uid("query2-semantic-trips-v1")
                .setParallelism(job.sourceParallelism());

        SingleOutputStreamOperator<CellDelta> taxiDeltas = semanticTrips
                .keyBy(SemanticTrip::medallion)
                .process(new Query2Functions.TaxiState())
                .name("Query 2 taxi state by medallion")
                .uid("query2-taxi-state-v1")
                .setParallelism(settings.taxiParallelism());

        SingleOutputStreamOperator<ProfitComponentUpdate> emptyCounts = taxiDeltas
                .keyBy(CellDelta::cellId)
                .process(new Query2Functions.EmptyCellState())
                .name("Query 2 empty taxi counts by cell")
                .uid("query2-empty-cell-state-v1")
                .setParallelism(settings.cellParallelism());

        SingleOutputStreamOperator<FareInput> fares = semanticTrips
                .map(Query2Functions::fareInput)
                .filter(Objects::nonNull)
                .returns(TypeInformation.of(FareInput.class))
                .name("Query 2 valid fare contributions")
                .uid("query2-valid-fares-v1")
                .setParallelism(job.sourceParallelism());

        SingleOutputStreamOperator<ProfitComponentUpdate> medians = fares
                .keyBy(FareInput::cellId)
                .process(new Query2Functions.FareWindow())
                .name("Query 2 exact 15-minute median by cell")
                .uid("query2-fare-median-v1")
                .setParallelism(settings.cellParallelism());

        SingleOutputStreamOperator<ProfitAggregate> profits = emptyCounts
                .union(medians)
                .keyBy(ProfitComponentUpdate::cellId)
                .process(new Query2Functions.ProfitCellState())
                .name("Query 2 profitability by cell")
                .uid("query2-profitability-v1")
                .setParallelism(settings.cellParallelism());

        SingleOutputStreamOperator<GlobalMessage> shardMessages = profits
                .keyBy(value -> Query2Functions.rankingShard(value.cellId(), settings.rankingShards()))
                .process(new Query2Functions.ShardRank())
                .returns(TypeInformation.of(GlobalMessage.class))
                .name("Query 2 shard-local top ten")
                .uid("query2-shard-top-v3")
                .setParallelism(settings.rankingParallelism())
                .setMaxParallelism(job.rankingMaxParallelism());

        SingleOutputStreamOperator<GlobalMessage> query2TimerBoundaries = profits
                .filter(ProfitAggregate::timer)
                .map(value -> (GlobalMessage) new BoundaryMessage(
                        value.effectiveTimeMillis(), false, true, value.runId(), value.datasetId(),
                        0L, 0L, 0L))
                .returns(TypeInformation.of(GlobalMessage.class))
                .name("Query 2 timer boundary signals")
                .uid("query2-timer-boundaries-v1")
                .setParallelism(settings.cellParallelism());

        SingleOutputStreamOperator<GlobalMessage> eosMessages = eos
                .map(value -> (GlobalMessage) new EosMessage(
                        value.eventTimeMillis(), value.partition(), value.expectedPartitionCount(),
                        value.runId(), value.datasetId()))
                .returns(TypeInformation.of(GlobalMessage.class))
                .name("Query 2 terminal boundary signals")
                .uid("query2-terminal-boundaries-v1")
                .setParallelism(job.sourceParallelism());

        return new DistributedComponents(
                shardMessages,
                query2TimerBoundaries,
                eosMessages);
    }

    public static SingleOutputStreamOperator<Query2Result> globalWithSharedBoundaries(
            DistributedComponents components,
            DataStream<GlobalMessage> sharedBoundaries,
            Query2Settings settings) {
        return components.shardMessages()
                .union(sharedBoundaries, components.eosMessages())
                .keyBy(ignored -> 0)
                .process(new Query2Functions.GlobalRank(settings.rankingShards()))
                .name("Query 2 exact global top ten")
                .uid("query2-global-top-v3")
                .setParallelism(1)
                .slotSharingGroup("query2-global");
    }

    public record DistributedComponents(
            DataStream<GlobalMessage> shardMessages,
            DataStream<GlobalMessage> timerBoundaries,
            DataStream<GlobalMessage> eosMessages) {}

}
