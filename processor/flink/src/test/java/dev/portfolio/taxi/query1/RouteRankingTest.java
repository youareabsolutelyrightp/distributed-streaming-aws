package dev.portfolio.taxi.query1;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.portfolio.taxi.query1.Query1Models.RouteRankEntry;

import java.util.List;

import org.junit.jupiter.api.Test;

final class RouteRankingTest {
    @Test
    void appliesAllFourTieBreaksAndMaintainsExactTen() {
        RouteRanking ranking = new RouteRanking();
        ranking.set(entry(2, 1, 100, 1));
        ranking.set(entry(1, 1, 100, 1));
        ranking.set(entry(3, 1, 101, 1));
        ranking.set(entry(4, 1, 101, 2));
        assertEquals(List.of(4L, 3L, 1L, 2L), ranking.top().stream()
                .map(RouteRankEntry::routeId)
                .toList());

        for (long route = 5; route <= 20; route++) {
            ranking.set(entry(route, route, 100, route));
        }
        assertEquals(10, ranking.top().size());
        assertEquals(20L, ranking.top().get(0).routeId());
        ranking.remove(20L);
        assertEquals(19L, ranking.top().get(0).routeId());
    }

    private static RouteRankEntry entry(long route, long count, long time, long line) {
        return new RouteRankEntry(route, 1, 1, count, time, line);
    }
}
