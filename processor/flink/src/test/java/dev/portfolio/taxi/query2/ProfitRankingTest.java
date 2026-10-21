package dev.portfolio.taxi.query2;

import static dev.portfolio.taxi.query2.Query2Models.ProfitRankEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

final class ProfitRankingTest {
    @Test
    void usesExactRatioThenFreshnessLineAndCell() {
        ProfitRanking ranking = new ProfitRanking();
        ranking.set(entry(1, 1, 3, 100, 1));
        ranking.set(entry(2, 2, 7, 100, 2));
        assertEquals(List.of(1, 2), cells(ranking));
        ranking.set(entry(3, 2, 6, 100, 3));
        assertEquals(List.of(3, 1, 2), cells(ranking));
        ranking.set(entry(4, 4, 12, 100, 2));
        assertEquals(List.of(3, 4, 1, 2), cells(ranking));
        ranking.set(entry(0, 4, 12, 100, 2));
        assertEquals(List.of(3, 0, 4, 1, 2), cells(ranking));
    }

    @Test
    void roundsPublicValueHalfEvenWithoutChangingRankMath() {
        assertEquals("0.005000", Query2Result.roundHalfEven(1, 200));
        assertEquals("0.002500", Query2Result.roundHalfEven(1, 400));
        assertEquals("0.166667", Query2Result.roundHalfEven(1, 6));
    }

    private static ProfitRankEntry entry(int cell, long money, long empty, long time, long line) {
        return new ProfitRankEntry(cell, money, empty, time, line);
    }
    private static List<Integer> cells(ProfitRanking ranking) {
        return ranking.top().stream().map(ProfitRankEntry::cellId).toList();
    }
}
