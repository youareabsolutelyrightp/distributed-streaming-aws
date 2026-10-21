package dev.portfolio.taxi.input;

import static dev.portfolio.taxi.input.InputModels.EosEvent;

import dev.portfolio.taxi.config.JobSettings;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.metrics.MetricGroup;

public final class InputWatermarks {
    private InputWatermarks() {}

    public static WatermarkStrategy<KafkaInputRecord> sourceStrategy(JobSettings settings) {
        WatermarkStrategy<KafkaInputRecord> strategy = WatermarkStrategy
                .<KafkaInputRecord>forGenerator(context -> new PartitionGenerator(context))
                .withTimestampAssigner((SerializableTimestampAssigner<KafkaInputRecord>)
                        (record, ignored) -> record.valid()
                                ? record.envelope().eventTimeMillis()
                                : Long.MIN_VALUE);
        if (settings.watermarkAlignmentMaxDriftMillis() > 0L) {
            strategy = strategy.withWatermarkAlignment(
                    "reconstructed-foil-input-v1",
                    Duration.ofMillis(settings.watermarkAlignmentMaxDriftMillis()),
                    Duration.ofMillis(settings.watermarkAlignmentUpdateMillis()));
        }
        return strategy;
    }

    private abstract static class CappedGenerator<T> implements WatermarkGenerator<T> {
        private long maximum = Long.MIN_VALUE;

        protected final void observe(long eventTimestamp) {
            maximum = Math.max(maximum, eventTimestamp);
        }

        protected final void terminal(long eventTimestamp, WatermarkOutput output) {
            if (maximum != Long.MIN_VALUE && eventTimestamp < maximum) {
                throw new IllegalArgumentException(
                        "terminal event time precedes an observed input event");
            }
            maximum = eventTimestamp;
            output.emitWatermark(new Watermark(eventTimestamp));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            if (maximum != Long.MIN_VALUE) {
                output.emitWatermark(new Watermark(
                        Math.subtractExact(maximum, 1L)));
            }
        }
    }

    private static final class PartitionGenerator extends CappedGenerator<KafkaInputRecord> {
        private final MetricGroup metricGroup;
        private final AtomicLong currentWatermark = new AtomicLong(Long.MIN_VALUE);
        private final AtomicLong maximumEventTime = new AtomicLong(Long.MIN_VALUE);
        private boolean metricsRegistered;
        private boolean terminalSeen;
        private int partition = -1;

        private PartitionGenerator(WatermarkGeneratorSupplier.Context context) {
            metricGroup = context.getMetricGroup().addGroup("query1_partition_watermarks");
        }

        @Override
        public void onEvent(
                KafkaInputRecord record, long eventTimestamp, WatermarkOutput output) {
            registerMetrics(record.kafkaPartition());
            if (terminalSeen) {
                throw new IllegalArgumentException(
                        "Kafka partition " + record.kafkaPartition()
                                + " contains a record after its EOS marker");
            }
            if (!record.valid()) {
                return;
            }
            maximumEventTime.accumulateAndGet(eventTimestamp, Math::max);
            if (record.envelope() instanceof EosEvent eos) {
                if (eos.partition() != record.kafkaPartition()) {
                    throw new IllegalArgumentException(
                            "EOS payload partition " + eos.partition()
                                    + " does not match Kafka partition " + record.kafkaPartition());
                }
                terminalSeen = true;
                terminal(eventTimestamp, new TrackingWatermarkOutput(output, currentWatermark));
            } else {
                observe(eventTimestamp);
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            super.onPeriodicEmit(new TrackingWatermarkOutput(output, currentWatermark));
        }

        private void registerMetrics(int observedPartition) {
            if (metricsRegistered) {
                if (partition != observedPartition) {
                    throw new IllegalStateException("one split watermark generator observed two Kafka partitions");
                }
                return;
            }
            partition = observedPartition;
            MetricGroup partitionMetrics = metricGroup.addGroup("partition", Integer.toString(partition));
            partitionMetrics.gauge(
                    "generated_watermark_before_alignment", currentWatermark::get);
            partitionMetrics.gauge("maximum_event_time", maximumEventTime::get);
            metricsRegistered = true;
        }
    }

    private record TrackingWatermarkOutput(WatermarkOutput delegate, AtomicLong observed)
            implements WatermarkOutput {
        @Override
        public void emitWatermark(Watermark watermark) {
            observed.accumulateAndGet(watermark.getTimestamp(), Math::max);
            delegate.emitWatermark(watermark);
        }

        @Override
        public void markIdle() {
            delegate.markIdle();
        }

        @Override
        public void markActive() {
            delegate.markActive();
        }
    }
}
