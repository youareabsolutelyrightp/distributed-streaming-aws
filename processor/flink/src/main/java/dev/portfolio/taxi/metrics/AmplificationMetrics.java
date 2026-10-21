package dev.portfolio.taxi.metrics;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;

public final class AmplificationMetrics {
    private AmplificationMetrics() {}

    public static Counter counter(RuntimeContext context, String stage) {
        return context.getMetricGroup()
                .addGroup("taxi_amplification")
                .addGroup("stage", stage)
                .counter("records_total");
    }
}
