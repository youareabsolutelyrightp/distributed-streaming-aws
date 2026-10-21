package dev.portfolio.taxi.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

final class GridTest {
    @Test
    void matchesWestNorthInclusiveAndEastSouthExclusiveEdges() {
        long west = Grid.CENTER_LONGITUDE - Grid.Q1_LONGITUDE_STEP / 2L;
        long north = Grid.CENTER_LATITUDE + Grid.Q1_LATITUDE_STEP / 2L;

        assertEquals(cell(1, 1), Grid.query1Cell(decimal(west), decimal(north)).orElseThrow());
        assertEquals(
                cell(2, 1),
                Grid.query1Cell(decimal(west + Grid.Q1_LONGITUDE_STEP), decimal(north)).orElseThrow());
        assertEquals(
                cell(1, 2),
                Grid.query1Cell(decimal(west), decimal(north - Grid.Q1_LATITUDE_STEP)).orElseThrow());

        long outsideEast = west + 300L * Grid.Q1_LONGITUDE_STEP;
        long outsideSouth = north - 300L * Grid.Q1_LATITUDE_STEP;
        assertTrue(Grid.query1Cell(decimal(outsideEast), decimal(north)).isEmpty());
        assertTrue(Grid.query1Cell(decimal(west), decimal(outsideSouth)).isEmpty());
    }

    @Test
    void rejectsMalformedAndOverpreciseCoordinates() {
        assertEquals(OptionalInt.empty(), Grid.query1Cell("", "41.474937"));
        assertEquals(OptionalInt.empty(), Grid.query1Cell("-74.0.", "41.474937"));
        assertEquals(OptionalInt.empty(), Grid.query1Cell("-74.9135850001", "41.474937"));
    }

    private static int cell(int east, int south) {
        return (east << Grid.CELL_SHIFT) | south;
    }

    private static String decimal(long nano) {
        boolean negative = nano < 0;
        long absolute = Math.abs(nano);
        return (negative ? "-" : "")
                + (absolute / Grid.NANO)
                + "."
                + String.format("%09d", absolute % Grid.NANO);
    }
}
