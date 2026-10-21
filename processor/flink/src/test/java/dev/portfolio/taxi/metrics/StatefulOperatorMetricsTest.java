package dev.portfolio.taxi.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

final class StatefulOperatorMetricsTest {
    @Test
    void tracksLogicalStateAndBufferedAge() {
        StatefulOperatorMetrics metrics = new StatefulOperatorMetrics();
        metrics.addActiveEntries(3L);
        metrics.rememberPendingBoundary(200L);
        metrics.rememberPendingBoundary(100L);
        metrics.addRegisteredEventTimeTimers(4L);
        long bufferedAt = metrics.rememberBufferedEvent();

        assertEquals(3L, metrics.activeStateEntries());
        assertEquals(2L, metrics.pendingBoundaryCount());
        assertEquals(100L, metrics.oldestPendingBoundaryEventTimeMillis());
        assertEquals(4L, metrics.registeredEventTimeTimerCount());
        assertEquals(1L, metrics.bufferedEventCount());
        metrics.forgetBufferedEvent(bufferedAt);
        metrics.forgetPendingBoundary(100L);
        assertEquals(200L, metrics.oldestPendingBoundaryEventTimeMillis());
        metrics.forgetPendingBoundary(200L);
        assertEquals(Long.MAX_VALUE, metrics.oldestPendingBoundaryEventTimeMillis());
        assertEquals(0L, metrics.bufferedEventCount());
        assertEquals(0L, metrics.oldestBufferedEventAgeMillis());
    }

    @Test
    void rejectsNegativeCounts() {
        StatefulOperatorMetrics metrics = new StatefulOperatorMetrics();
        assertThrows(IllegalStateException.class, () -> metrics.addActiveEntries(-1L));
        assertThrows(IllegalStateException.class, () -> metrics.forgetPendingBoundary(1L));
        assertThrows(
                IllegalStateException.class,
                () -> metrics.addRegisteredEventTimeTimers(-1L));
    }
}
