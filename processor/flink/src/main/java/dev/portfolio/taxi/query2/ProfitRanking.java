package dev.portfolio.taxi.query2;

import static dev.portfolio.taxi.query2.Query2Models.ProfitRankEntry;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ProfitRanking implements Serializable {
    public static final long FARE_WINDOW_MILLIS = 15L * 60L * 1000L;
    public static final long TAXI_WINDOW_MILLIS = 30L * 60L * 1000L;
    public static final Comparator<ProfitRankEntry> ORDER = new ExactOrder();

    private final Map<Integer, ProfitRankEntry> current = new HashMap<>();

    public void set(ProfitRankEntry entry) {
        if (entry.emptyTaxis() <= 0L || entry.medianTwiceCents() < 0L) {
            throw new IllegalArgumentException("profit entry must have non-negative money and an empty taxi");
        }
        current.put(entry.cellId(), entry);
    }

    public void remove(int cellId) {
        current.remove(cellId);
    }

    public List<ProfitRankEntry> top() {
        ArrayList<ProfitRankEntry> values = new ArrayList<>(current.values());
        values.sort(ORDER);
        return List.copyOf(values.subList(0, Math.min(10, values.size())));
    }

    public boolean contains(int cellId) {
        return current.containsKey(cellId);
    }

    public int size() {
        return current.size();
    }

    public static String signature(List<ProfitRankEntry> top) {
        StringBuilder value = new StringBuilder(top.size() * 64);
        for (ProfitRankEntry entry : top) {
            value.append(entry.cellId()).append(':')
                    .append(entry.medianTwiceCents()).append(':')
                    .append(entry.emptyTaxis()).append(':')
                    .append(entry.freshnessTimeMillis()).append(':')
                    .append(entry.freshnessSourceLine()).append(';');
        }
        return value.toString();
    }

    private static final class ExactOrder implements Comparator<ProfitRankEntry>, Serializable {
        @Override
        public int compare(ProfitRankEntry left, ProfitRankEntry right) {
            BigInteger leftCross = BigInteger.valueOf(left.medianTwiceCents())
                    .multiply(BigInteger.valueOf(right.emptyTaxis()));
            BigInteger rightCross = BigInteger.valueOf(right.medianTwiceCents())
                    .multiply(BigInteger.valueOf(left.emptyTaxis()));
            int byRatio = rightCross.compareTo(leftCross);
            if (byRatio != 0) {
                return byRatio;
            }
            int byTime = Long.compare(right.freshnessTimeMillis(), left.freshnessTimeMillis());
            if (byTime != 0) {
                return byTime;
            }
            int byLine = Long.compare(right.freshnessSourceLine(), left.freshnessSourceLine());
            return byLine != 0 ? byLine : Integer.compare(left.cellId(), right.cellId());
        }
    }
}
