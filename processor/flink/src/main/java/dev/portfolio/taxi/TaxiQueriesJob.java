package dev.portfolio.taxi;

import static dev.portfolio.taxi.input.InputModels.InputEnvelope;

import dev.portfolio.taxi.config.JobSettings;
import dev.portfolio.taxi.control.CompletionSignals;
import dev.portfolio.taxi.control.RunControlKafkaSchema;
import dev.portfolio.taxi.control.RunControlRecord;
import dev.portfolio.taxi.boundary.SharedBoundaryPipeline;
import dev.portfolio.taxi.input.InputReplayDeduplicator;
import dev.portfolio.taxi.input.InputValidator;
import dev.portfolio.taxi.input.InputWatermarks;
import dev.portfolio.taxi.input.KafkaInputRecord;
import dev.portfolio.taxi.input.TaxiKafkaDeserializer;
import dev.portfolio.taxi.measurement.KafkaResultSchemas;
import dev.portfolio.taxi.query1.Query1Pipeline;
import dev.portfolio.taxi.query1.Query1Result;
import dev.portfolio.taxi.query1.Query1Settings;
import dev.portfolio.taxi.query2.Query2Pipeline;
import dev.portfolio.taxi.query2.Query2Result;
import dev.portfolio.taxi.query2.Query2Settings;

import java.util.Map;

import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class TaxiQueriesJob {
    private TaxiQueriesJob() {}

    public static void main(String[] arguments) throws Exception {
        Map<String, String> environmentVariables = System.getenv();
        JobSettings job = JobSettings.fromEnvironment(environmentVariables);
        Query1Settings query1 = Query1Settings.fromEnvironment(environmentVariables);
        Query2Settings query2 = Query2Settings.fromEnvironment(environmentVariables);
        job.validateRankingParallelism(
                query1.rankingParallelism(), query2.rankingParallelism());

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        registerBenchmarkPlacementGroups(environment);
        Configuration checkpointConfiguration = new Configuration();
        checkpointConfiguration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                job.checkpointDirectory());
        environment.configure(checkpointConfiguration);
        environment.setParallelism(Math.max(job.sourceParallelism(), query2.cellParallelism()));
        environment.getConfig().setAutoWatermarkInterval(50L);
        environment.enableCheckpointing(
                job.checkpointIntervalMillis(),
                CheckpointingMode.EXACTLY_ONCE);
        environment.getCheckpointConfig().setCheckpointTimeout(
                job.checkpointTimeoutMillis());
        environment.getCheckpointConfig().setTolerableCheckpointFailureNumber(
                job.tolerableCheckpointFailures());
        environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(500L);
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        KafkaSource<KafkaInputRecord> source = KafkaSource.<KafkaInputRecord>builder()
                .setBootstrapServers(job.bootstrapServers())
                .setTopics(job.inputTopic())
                .setGroupId(job.groupId())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new TaxiKafkaDeserializer())
                .setProperty("partition.discovery.interval.ms", "-1")
                .build();

        DataStream<KafkaInputRecord> raw = environment
                .fromSource(source, InputWatermarks.sourceStrategy(job), "Shared taxi Kafka source")
                .uid("taxi-shared-kafka-source-v1")
                .setParallelism(job.sourceParallelism())
                .slotSharingGroup("source");

        DataStream<KafkaInputRecord> deduplicated = raw
                .flatMap(new InputReplayDeduplicator(job.inputPartitions()))
                .returns(TypeInformation.of(KafkaInputRecord.class))
                .name("Deduplicate shared taxi input replays")
                .uid("taxi-shared-input-dedup-v2")
                .setParallelism(job.sourceParallelism())
                .slotSharingGroup("source");

        SingleOutputStreamOperator<InputEnvelope> parsed = deduplicated
                .process(new InputValidator())
                .returns(TypeInformation.of(InputEnvelope.class))
                .name("Validate shared taxi input and lateness")
                .uid("taxi-shared-input-parser-v1")
                .setParallelism(job.sourceParallelism())
                .slotSharingGroup("source");

        KafkaSink<String> dlq = KafkaSink.<String>builder()
                .setBootstrapServers(job.bootstrapServers())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(new KafkaResultSchemas.AppendTimeString(job.dlqTopic()))
                .build();
        parsed.getSideOutput(InputValidator.MALFORMED)
                .union(parsed.getSideOutput(InputValidator.LATE_EVENTS))
                .sinkTo(dlq)
                .name("Shared rejected input DLQ")
                .uid("taxi-shared-dlq-sink-v1")
                .setParallelism(job.sourceParallelism())
                .slotSharingGroup("source");

        Query1Pipeline.DistributedComponents query1Components =
                Query1Pipeline.distributedComponents(
                        parsed,
                        job,
                        query1,
                        "source");
        Query2Pipeline.DistributedComponents query2Components =
                Query2Pipeline.distributedComponents(parsed, job, query2);
        var sharedBoundaries = SharedBoundaryPipeline.build(
                query1Components.inputBoundaries(),
                query1Components.timerBoundaries(),
                query2Components.timerBoundaries(),
                job.boundaryParallelism());
        var query1Boundaries = SharedBoundaryPipeline.forQuery1(
                sharedBoundaries, job.boundaryParallelism());
        var query2Boundaries = SharedBoundaryPipeline.forQuery2(
                sharedBoundaries, job.boundaryParallelism());
        SingleOutputStreamOperator<Query1Result> query1Results =
                Query1Pipeline.globalWithSharedBoundaries(query1Components, query1Boundaries);
        SingleOutputStreamOperator<Query2Result> query2Results =
                Query2Pipeline.globalWithSharedBoundaries(query2Components, query2Boundaries, query2);
        query1Results
                .sinkTo(KafkaSink.<Query1Result>builder()
                        .setBootstrapServers(job.bootstrapServers())
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setRecordSerializer(new KafkaResultSchemas.Query1(query1.outputTopic()))
                        .build())
                .name("Query 1 canonical Kafka sink")
                .uid("taxi-query1-result-sink-v1")
                .setParallelism(1)
                .slotSharingGroup("query1-global");

        query2Results
                .sinkTo(KafkaSink.<Query2Result>builder()
                        .setBootstrapServers(job.bootstrapServers())
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setRecordSerializer(new KafkaResultSchemas.Query2(query2.outputTopic()))
                        .build())
                .name("Query 2 canonical Kafka sink")
                .uid("taxi-query2-result-sink-v1")
                .setParallelism(1)
                .slotSharingGroup("query2-global");

        query1Results.getSideOutput(CompletionSignals.QUERY1)
                .union(query2Results.getSideOutput(CompletionSignals.QUERY2))
                .sinkTo(KafkaSink.<RunControlRecord>builder()
                        .setBootstrapServers(job.bootstrapServers())
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setRecordSerializer(new RunControlKafkaSchema(job.controlTopic()))
                        .build())
                .name("Query completion Kafka sink")
                .uid("taxi-query-completion-sink-v1")
                .setParallelism(1)
                .slotSharingGroup("query1-global");

        environment.execute("reconstructed-foil-both-queries-event-time-state");
    }

    static void registerBenchmarkPlacementGroups(StreamExecutionEnvironment environment) {
        environment.registerSlotSharingGroup(SlotSharingGroup.newBuilder("default")
                .setCpuCores(1.34)
                .setTaskHeapMemoryMB(256)
                .setExternalResource("benchmark-placement", 1.0)
                .build());
        environment.registerSlotSharingGroup(SlotSharingGroup.newBuilder("source")
                .setCpuCores(0.01)
                .setTaskHeapMemoryMB(128)
                .setExternalResource("benchmark-placement", 51.0)
                .build());
        environment.registerSlotSharingGroup(SlotSharingGroup.newBuilder("query1-global")
                .setCpuCores(0.67)
                .setTaskHeapMemoryMB(128)
                .setExternalResource("benchmark-placement", 24.0)
                .build());
        environment.registerSlotSharingGroup(SlotSharingGroup.newBuilder("query2-global")
                .setCpuCores(0.67)
                .setTaskHeapMemoryMB(128)
                .setExternalResource("benchmark-placement", 24.0)
                .build());
    }

}
