package dev.portfolio.taxi.query2;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

final class ExactMedianWindowTest {
    @Test
    void handlesEvenOddDuplicatesAndRemoval() {
        ExactMedianWindow window = new ExactMedianWindow();
        window.add(100); assertEquals(200, window.medianTwiceCents());
        window.add(200); assertEquals(300, window.medianTwiceCents());
        window.add(200); assertEquals(400, window.medianTwiceCents());
        window.add(300); assertEquals(400, window.medianTwiceCents());
        window.remove(200); assertEquals(400, window.medianTwiceCents());
        window.remove(100); assertEquals(500, window.medianTwiceCents());
        window.remove(200); assertEquals(600, window.medianTwiceCents());
        window.remove(300); assertEquals(0, window.size());
    }

    @Test
    void matchesSortedValuesAcrossRandomUpdates() {
        Random random = new Random(20260803L);
        ExactMedianWindow window = new ExactMedianWindow();
        List<Long> values = new ArrayList<>();
        for (int iteration = 0; iteration < 2_000; iteration++) {
            if (!values.isEmpty() && random.nextDouble() < 0.48) {
                long value = values.remove(random.nextInt(values.size()));
                window.remove(value);
            } else {
                long value = random.nextInt(500);
                values.add(value);
                window.add(value);
            }
            assertEquals(values.size(), window.size());
            if (!values.isEmpty()) {
                List<Long> ordered = new ArrayList<>(values);
                Collections.sort(ordered);
                int middle = ordered.size() / 2;
                long expected = ordered.size() % 2 == 1
                        ? ordered.get(middle) * 2L
                        : ordered.get(middle - 1) + ordered.get(middle);
                assertEquals(expected, window.medianTwiceCents());
            }
        }
    }
}
