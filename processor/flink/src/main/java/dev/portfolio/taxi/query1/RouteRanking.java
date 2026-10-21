package dev.portfolio.taxi.query1;

import static dev.portfolio.taxi.query1.Query1Models.RouteRankEntry;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

public final class RouteRanking implements Serializable {
    public static final long WINDOW_MILLIS = 30L * 60L * 1000L;
    public static final Comparator<RouteRankEntry> ORDER =
            new RouteEntryOrder();

    private final Map<Long, VersionedEntry> current = new HashMap<>();
    private final PriorityQueue<VersionedEntry> heap =
            new PriorityQueue<>(new VersionedEntryOrder());
    private List<RouteRankEntry> top = List.of();
    private Set<Long> topKeys = Set.of();
    private long version;

    public void set(RouteRankEntry entry) {
        boolean wasTop = topKeys.contains(entry.routeId());
        VersionedEntry versioned = new VersionedEntry(entry, ++version);
        current.put(entry.routeId(), versioned);
        heap.add(versioned);
        if (wasTop || top.size() < 10 || (!top.isEmpty() && ORDER.compare(entry, top.get(top.size() - 1)) < 0)) {
            rebuildTop();
        }
        compactIfNeeded();
    }

    public void remove(long routeId) {
        boolean wasTop = topKeys.contains(routeId);
        current.remove(routeId);
        if (wasTop) {
            rebuildTop();
        }
        compactIfNeeded();
    }

    public List<RouteRankEntry> top() {
        return List.copyOf(top);
    }

    public boolean contains(long routeId) {
        return current.containsKey(routeId);
    }

    public int size() {
        return current.size();
    }

    public static String signature(List<RouteRankEntry> top) {
        StringBuilder value = new StringBuilder(top.size() * 48);
        for (RouteRankEntry entry : top) {
            value.append(entry.routeId()).append(':')
                    .append(entry.count()).append(':')
                    .append(entry.freshnessTimeMillis()).append(':')
                    .append(entry.freshnessSourceLine()).append(';');
        }
        return value.toString();
    }

    private void rebuildTop() {
        List<VersionedEntry> selected = new ArrayList<>(10);
        while (!heap.isEmpty() && selected.size() < 10) {
            VersionedEntry candidate = heap.remove();
            VersionedEntry active = current.get(candidate.entry().routeId());
            if (active != null && active.version() == candidate.version()) {
                selected.add(candidate);
            }
        }
        heap.addAll(selected);
        top = selected.stream().map(VersionedEntry::entry).toList();
        Set<Long> keys = new HashSet<>();
        for (RouteRankEntry entry : top) {
            keys.add(entry.routeId());
        }
        topKeys = keys;
    }

    private void compactIfNeeded() {
        int limit = Math.max(4096, current.size() * 4);
        if (heap.size() <= limit) {
            return;
        }
        heap.clear();
        heap.addAll(current.values());
        rebuildTop();
    }

    private record VersionedEntry(RouteRankEntry entry, long version) implements Serializable {}

    private static final class RouteEntryOrder
            implements Comparator<RouteRankEntry>, Serializable {
        @Override
        public int compare(RouteRankEntry left, RouteRankEntry right) {
            int byCount = Long.compare(right.count(), left.count());
            if (byCount != 0) {
                return byCount;
            }
            int byTime = Long.compare(right.freshnessTimeMillis(), left.freshnessTimeMillis());
            if (byTime != 0) {
                return byTime;
            }
            int byLine = Long.compare(right.freshnessSourceLine(), left.freshnessSourceLine());
            return byLine != 0 ? byLine : Long.compare(left.routeId(), right.routeId());
        }
    }

    private static final class VersionedEntryOrder
            implements Comparator<VersionedEntry>, Serializable {
        @Override
        public int compare(VersionedEntry left, VersionedEntry right) {
            return ORDER.compare(left.entry(), right.entry());
        }
    }
}
