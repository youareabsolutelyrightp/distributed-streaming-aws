package dev.portfolio.taxi.query2;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

public final class ExactMedianWindow implements Serializable {
    private final TreeMap<Long, Integer> lower = new TreeMap<>();
    private final TreeMap<Long, Integer> upper = new TreeMap<>();
    private long lowerSize;
    private long upperSize;

    public long size() {
        return lowerSize + upperSize;
    }

    public void add(long value) {
        if (lowerSize == 0 || value <= lower.lastKey()) {
            addCount(lower, value);
            lowerSize++;
        } else {
            addCount(upper, value);
            upperSize++;
        }
        rebalance();
    }

    public void remove(long value) {
        if (removeCount(lower, value)) {
            lowerSize--;
        } else if (removeCount(upper, value)) {
            upperSize--;
        } else {
            throw new IllegalArgumentException("median window does not contain " + value);
        }
        rebalance();
    }

    public long medianTwiceCents() {
        if (size() == 0) {
            throw new IllegalStateException("empty median window");
        }
        if (lowerSize > upperSize) {
            return Math.multiplyExact(lower.lastKey(), 2L);
        }
        return Math.addExact(lower.lastKey(), upper.firstKey());
    }

    private void rebalance() {
        while (lowerSize > upperSize + 1L) {
            long value = lower.lastKey();
            removeCount(lower, value);
            lowerSize--;
            addCount(upper, value);
            upperSize++;
        }
        while (lowerSize < upperSize) {
            long value = upper.firstKey();
            removeCount(upper, value);
            upperSize--;
            addCount(lower, value);
            lowerSize++;
        }
    }

    private static void addCount(Map<Long, Integer> values, long value) {
        values.merge(value, 1, Integer::sum);
    }

    private static boolean removeCount(Map<Long, Integer> values, long value) {
        Integer count = values.get(value);
        if (count == null) {
            return false;
        }
        if (count == 1) {
            values.remove(value);
        } else {
            values.put(value, count - 1);
        }
        return true;
    }
}
