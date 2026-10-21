package dev.portfolio.taxi.query1;

import static dev.portfolio.taxi.query1.Query1Models.RouteRankEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

final class GlobalTopTenMergeTest {
    @Test
    void exactMergeMatchesFullSortForEveryCandidateShardCount() {
        for (int shards : List.of(24, 48, 64, 96)) {
            ArrayList<RouteRankEntry> all = entries();
            ArrayList<List<RouteRankEntry>> shardTops = new ArrayList<>();
            for (int shard = 0; shard < shards; shard++) {
                int selectedShard = shard;
                List<RouteRankEntry> top = all.stream()
                        .filter(value -> Query1Functions.rankingShard(value.routeId(), shards)
                                == selectedShard)
                        .sorted(RouteRanking.ORDER)
                        .limit(10)
                        .toList();
                if (!top.isEmpty()) {
                    shardTops.add(top);
                }
            }
            List<RouteRankEntry> expected = all.stream()
                    .sorted(RouteRanking.ORDER)
                    .limit(10)
                    .toList();
            assertEquals(expected, GlobalTopTenMerger.merge(shardTops));
        }
    }

    private static ArrayList<RouteRankEntry> entries() {
        ArrayList<RouteRankEntry> values = new ArrayList<>();
        for (int route = 1; route <= 250; route++) {
            long count = 1L + Math.floorMod(route * 37, 50);
            long freshnessTime = 10_000L + Math.floorMod(route, 7);
            long freshnessLine = 50_000L + Math.floorMod(route, 5);
            values.add(new RouteRankEntry(
                    route, route, route + 1, count, freshnessTime, freshnessLine));
        }
        values.add(new RouteRankEntry(1_001L, 10, 11, 80L, 20_000L, 80_000L));
        values.add(new RouteRankEntry(1_002L, 12, 13, 80L, 20_000L, 80_001L));
        values.add(new RouteRankEntry(1_003L, 14, 15, 80L, 20_000L, 80_001L));
        return values;
    }
}
