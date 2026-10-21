package dev.portfolio.taxi.query1;

import static dev.portfolio.taxi.query1.Query1Models.RouteRankEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public final class GlobalTopTenMerger {
    private GlobalTopTenMerger() {}

    public static List<RouteRankEntry> merge(List<List<RouteRankEntry>> shardTops) {
        PriorityQueue<ShardCursor> candidates = new PriorityQueue<>((left, right) -> {
            int byRank = RouteRanking.ORDER.compare(left.entry(), right.entry());
            if (byRank != 0) return byRank;
            int byShard = Integer.compare(left.shardIndex(), right.shardIndex());
            return byShard != 0 ? byShard : Integer.compare(left.entryIndex(), right.entryIndex());
        });
        for (int shard = 0; shard < shardTops.size(); shard++) {
            List<RouteRankEntry> top = shardTops.get(shard);
            if (!top.isEmpty()) {
                candidates.add(new ShardCursor(shard, 0, top.get(0), top));
            }
        }
        ArrayList<RouteRankEntry> merged = new ArrayList<>(10);
        while (merged.size() < 10 && !candidates.isEmpty()) {
            ShardCursor cursor = candidates.remove();
            merged.add(cursor.entry());
            int next = cursor.entryIndex() + 1;
            if (next < cursor.entries().size()) {
                candidates.add(new ShardCursor(
                        cursor.shardIndex(), next, cursor.entries().get(next), cursor.entries()));
            }
        }
        return List.copyOf(merged);
    }

    private record ShardCursor(
            int shardIndex,
            int entryIndex,
            RouteRankEntry entry,
            List<RouteRankEntry> entries) {}
}
