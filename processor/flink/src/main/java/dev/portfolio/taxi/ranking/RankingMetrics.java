package dev.portfolio.taxi.ranking;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;

public final class RankingMetrics {
    private final RuntimeContext context;
    private final String query;
    private final Map<Integer, Counter> virtualShardInputs = new HashMap<>();
    private final Counter inputs;
    private final Counter snapshots;

    public RankingMetrics(RuntimeContext context, String query) {
        this.context = context;
        this.query = query;
        var group = context.getMetricGroup().addGroup("taxi_ranking").addGroup("query", query);
        inputs = group.counter("input_records_total");
        snapshots = group.counter("changed_snapshots_total");
    }

    public void input(int virtualShard) {
        inputs.inc();
        virtualShardInputs.computeIfAbsent(virtualShard, this::virtualShardCounter).inc();
    }

    public void snapshot() {
        snapshots.inc();
    }

    private Counter virtualShardCounter(int virtualShard) {
        return context.getMetricGroup()
                .addGroup("taxi_ranking")
                .addGroup("query", query)
                .addGroup("virtual_shard", Integer.toString(virtualShard))
                .counter("input_records_total");
    }
}
