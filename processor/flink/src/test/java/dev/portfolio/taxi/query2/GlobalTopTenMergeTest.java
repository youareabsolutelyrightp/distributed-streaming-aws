package dev.portfolio.taxi.query2;

import static dev.portfolio.taxi.query2.Query2Models.ProfitRankEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

final class GlobalTopTenMergeTest {
    @Test
    void exactMergeMatchesFullSortForEveryCandidateShardCount() {
        for (int shards : List.of(24, 48, 64, 96)) {
            ArrayList<ProfitRankEntry> all = entries();
            ArrayList<List<ProfitRankEntry>> shardTops = new ArrayList<>();
            for (int shard = 0; shard < shards; shard++) {
                int selectedShard = shard;
                List<ProfitRankEntry> top = all.stream()
                        .filter(value -> Query2Functions.rankingShard(value.cellId(), shards)
                                == selectedShard)
                        .sorted(ProfitRanking.ORDER)
                        .limit(10)
                        .toList();
                if (!top.isEmpty()) shardTops.add(top);
            }
            List<ProfitRankEntry> expected = all.stream()
                    .sorted(ProfitRanking.ORDER)
                    .limit(10)
                    .toList();
            assertEquals(expected, GlobalTopTenMerger.merge(shardTops));
        }
    }

    private static ArrayList<ProfitRankEntry> entries() {
        ArrayList<ProfitRankEntry> values = new ArrayList<>();
        for (int cell = 1; cell <= 250; cell++) {
            long median = 100L + Math.floorMod(cell * 37, 500);
            long empty = 1L + Math.floorMod(cell * 13, 11);
            long freshnessTime = 10_000L + Math.floorMod(cell, 7);
            long freshnessLine = 50_000L + Math.floorMod(cell, 5);
            values.add(new ProfitRankEntry(
                    cell, median, empty, freshnessTime, freshnessLine));
        }
        values.add(new ProfitRankEntry(1_001, 400L, 4L, 20_000L, 80_000L));
        values.add(new ProfitRankEntry(1_002, 500L, 5L, 20_000L, 80_001L));
        values.add(new ProfitRankEntry(1_003, 600L, 6L, 20_000L, 80_001L));
        return values;
    }
}
