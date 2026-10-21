package dev.portfolio.taxi.query2;

import static dev.portfolio.taxi.query2.Query2Models.ProfitRankEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public final class GlobalTopTenMerger {
    private GlobalTopTenMerger() {}

    public static List<ProfitRankEntry> merge(List<List<ProfitRankEntry>> shardTops) {
        PriorityQueue<ShardCursor> candidates = new PriorityQueue<>((left, right) -> {
            int byRank = ProfitRanking.ORDER.compare(left.entry(), right.entry());
            if (byRank != 0) return byRank;
            int byShard = Integer.compare(left.shardIndex(), right.shardIndex());
            return byShard != 0 ? byShard : Integer.compare(left.entryIndex(), right.entryIndex());
        });
        for (int shard = 0; shard < shardTops.size(); shard++) {
            List<ProfitRankEntry> top = shardTops.get(shard);
            if (!top.isEmpty()) {
                candidates.add(new ShardCursor(shard, 0, top.get(0), top));
            }
        }
        ArrayList<ProfitRankEntry> merged = new ArrayList<>(10);
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
            ProfitRankEntry entry,
            List<ProfitRankEntry> entries) {}
}
