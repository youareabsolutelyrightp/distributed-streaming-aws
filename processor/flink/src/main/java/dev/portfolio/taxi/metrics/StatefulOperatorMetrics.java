package dev.portfolio.taxi.metrics;

import java.io.Serializable;
import java.util.TreeMap;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;

public final class StatefulOperatorMetrics implements Serializable {
    private transient ListState<Snapshot> snapshotState;
    private long activeStateEntries;
    private long bufferedEventCount;
    private long pendingBoundaryCount;
    private long registeredEventTimeTimerCount;
    private TreeMap<Long, Integer> bufferedArrivalTimes = new TreeMap<>();
    private TreeMap<Long, Integer> pendingBoundaryTimes = new TreeMap<>();

    public void open(RuntimeContext context, String stage) {
        var metrics = context.getMetricGroup().addGroup("taxi_state").addGroup("stage", stage);
        metrics.gauge("active_state_entries", () -> activeStateEntries);
        metrics.gauge("buffered_event_count", () -> bufferedEventCount);
        metrics.gauge("pending_boundary_count", () -> pendingBoundaryCount);
        metrics.gauge("oldest_pending_boundary_event_time_ms", this::oldestPendingBoundaryEventTimeMillis);
        metrics.gauge("registered_event_time_timer_count", () -> registeredEventTimeTimerCount);
        metrics.gauge("oldest_buffered_event_age_ms", this::oldestBufferedEventAgeMillis);
    }

    public void initialize(FunctionInitializationContext context, String stateName) throws Exception {
        snapshotState = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>(stateName, TypeInformation.of(Snapshot.class)));
        activeStateEntries = 0L;
        bufferedEventCount = 0L;
        pendingBoundaryCount = 0L;
        registeredEventTimeTimerCount = 0L;
        bufferedArrivalTimes = new TreeMap<>();
        pendingBoundaryTimes = new TreeMap<>();
        if (!context.isRestored()) {
            return;
        }
        for (Snapshot snapshot : snapshotState.get()) {
            activeStateEntries += snapshot.activeStateEntries;
            bufferedEventCount += snapshot.bufferedEventCount;
            pendingBoundaryCount += snapshot.pendingBoundaryCount;
            registeredEventTimeTimerCount += snapshot.registeredEventTimeTimerCount;
            for (var entry : snapshot.bufferedArrivalTimes.entrySet()) {
                bufferedArrivalTimes.merge(entry.getKey(), entry.getValue(), Integer::sum);
            }
            if (snapshot.pendingBoundaryTimes != null) {
                for (var entry : snapshot.pendingBoundaryTimes.entrySet()) {
                    pendingBoundaryTimes.merge(entry.getKey(), entry.getValue(), Integer::sum);
                }
            }
        }
    }

    public void snapshot(FunctionSnapshotContext ignored) throws Exception {
        snapshotState.clear();
        snapshotState.add(new Snapshot(
                activeStateEntries,
                bufferedEventCount,
                pendingBoundaryCount,
                registeredEventTimeTimerCount,
                new TreeMap<>(bufferedArrivalTimes),
                new TreeMap<>(pendingBoundaryTimes)));
    }

    public void addActiveEntries(long delta) {
        activeStateEntries = nonNegative(activeStateEntries, delta, "active state entries");
    }

    public long rememberBufferedEvent() {
        long bufferedAt = System.currentTimeMillis();
        bufferedEventCount++;
        bufferedArrivalTimes.merge(bufferedAt, 1, Integer::sum);
        return bufferedAt;
    }

    public void forgetBufferedEvent(long bufferedAt) {
        forgetBufferedEvents(bufferedAt, 1);
    }

    public void forgetBufferedEvents(long bufferedAt, int count) {
        if (count <= 0) {
            return;
        }
        bufferedEventCount = nonNegative(bufferedEventCount, -count, "buffered events");
        Integer known = bufferedArrivalTimes.get(bufferedAt);
        if (known == null || known <= count) {
            bufferedArrivalTimes.remove(bufferedAt);
        } else {
            bufferedArrivalTimes.put(bufferedAt, known - count);
        }
    }

    public void rememberPendingBoundary(long eventTimeMillis) {
        pendingBoundaryCount++;
        pendingBoundaryTimes.merge(eventTimeMillis, 1, Integer::sum);
    }

    public void forgetPendingBoundary(long eventTimeMillis) {
        Integer known = pendingBoundaryTimes.get(eventTimeMillis);
        if (known == null) {
            throw new IllegalStateException("pending boundary time was not registered");
        }
        pendingBoundaryCount = nonNegative(pendingBoundaryCount, -1L, "pending boundaries");
        if (known == 1) {
            pendingBoundaryTimes.remove(eventTimeMillis);
        } else {
            pendingBoundaryTimes.put(eventTimeMillis, known - 1);
        }
    }

    public void addRegisteredEventTimeTimers(long delta) {
        registeredEventTimeTimerCount = nonNegative(
                registeredEventTimeTimerCount, delta, "registered event-time timers");
    }

    public long activeStateEntries() {
        return activeStateEntries;
    }

    public long bufferedEventCount() {
        return bufferedEventCount;
    }

    public long pendingBoundaryCount() {
        return pendingBoundaryCount;
    }

    public long registeredEventTimeTimerCount() {
        return registeredEventTimeTimerCount;
    }

    public long oldestBufferedEventAgeMillis() {
        if (bufferedArrivalTimes.isEmpty()) {
            return 0L;
        }
        return Math.max(0L, System.currentTimeMillis() - bufferedArrivalTimes.firstKey());
    }

    public long oldestPendingBoundaryEventTimeMillis() {
        return pendingBoundaryTimes.isEmpty() ? Long.MAX_VALUE : pendingBoundaryTimes.firstKey();
    }

    private static long nonNegative(long current, long delta, String name) {
        long result = Math.addExact(current, delta);
        if (result < 0L) {
            throw new IllegalStateException(name + " became negative");
        }
        return result;
    }

    public static final class Snapshot implements Serializable {
        private long activeStateEntries;
        private long bufferedEventCount;
        private long pendingBoundaryCount;
        private long registeredEventTimeTimerCount;
        private TreeMap<Long, Integer> bufferedArrivalTimes = new TreeMap<>();
        private TreeMap<Long, Integer> pendingBoundaryTimes = new TreeMap<>();

        public Snapshot() {}

        private Snapshot(
                long activeStateEntries,
                long bufferedEventCount,
                long pendingBoundaryCount,
                long registeredEventTimeTimerCount,
                TreeMap<Long, Integer> bufferedArrivalTimes,
                TreeMap<Long, Integer> pendingBoundaryTimes) {
            this.activeStateEntries = activeStateEntries;
            this.bufferedEventCount = bufferedEventCount;
            this.pendingBoundaryCount = pendingBoundaryCount;
            this.registeredEventTimeTimerCount = registeredEventTimeTimerCount;
            this.bufferedArrivalTimes = bufferedArrivalTimes;
            this.pendingBoundaryTimes = pendingBoundaryTimes;
        }
    }
}
