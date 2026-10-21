package dev.portfolio.taxi.model;

import java.util.OptionalInt;

public final class Grid {
    static final long NANO = 1_000_000_000L;
    static final long CENTER_LATITUDE = 41_474_937_000L;
    static final long CENTER_LONGITUDE = -74_913_585_000L;
    static final long Q1_LATITUDE_STEP = 4_491_556L;
    static final long Q1_LONGITUDE_STEP = 5_986_000L;
    static final long Q2_LATITUDE_STEP = Q1_LATITUDE_STEP / 2L;
    static final long Q2_LONGITUDE_STEP = Q1_LONGITUDE_STEP / 2L;
    static final int CELL_SHIFT = 10;
    static final int CELL_MASK = (1 << CELL_SHIFT) - 1;
    static final int ROUTE_SHIFT = 20;

    private Grid() {}

    public static OptionalInt query1Cell(String longitude, String latitude) {
        return cell(longitude, latitude, 300, Q1_LONGITUDE_STEP, Q1_LATITUDE_STEP);
    }

    public static OptionalInt query2Cell(String longitude, String latitude) {
        return cell(longitude, latitude, 600, Q2_LONGITUDE_STEP, Q2_LATITUDE_STEP);
    }

    private static OptionalInt cell(
            String longitude,
            String latitude,
            int size,
            long longitudeStep,
            long latitudeStep) {
        Long longitudeNano = parseDecimalNano(longitude);
        Long latitudeNano = parseDecimalNano(latitude);
        if (longitudeNano == null || latitudeNano == null) {
            return OptionalInt.empty();
        }
        long westEdge = CENTER_LONGITUDE - longitudeStep / 2L;
        long northEdge = CENTER_LATITUDE + latitudeStep / 2L;
        long eastOffset = longitudeNano - westEdge;
        long southOffset = northEdge - latitudeNano;
        if (eastOffset < 0 || southOffset < 0) {
            return OptionalInt.empty();
        }
        long eastIndex = eastOffset / longitudeStep + 1L;
        long southIndex = southOffset / latitudeStep + 1L;
        if (eastIndex > size || southIndex > size) {
            return OptionalInt.empty();
        }
        return OptionalInt.of((int) ((eastIndex << CELL_SHIFT) | southIndex));
    }

    static Long parseDecimalNano(String value) {
        if (value == null) {
            return null;
        }
        String text = value.trim();
        if (text.isEmpty()) {
            return null;
        }
        boolean negative = text.charAt(0) == '-';
        if (text.charAt(0) == '-' || text.charAt(0) == '+') {
            text = text.substring(1);
        }
        int separator = text.indexOf('.');
        String whole = separator < 0 ? text : text.substring(0, separator);
        String fraction = separator < 0 ? "" : text.substring(separator + 1);
        if (whole.isEmpty()
                || (separator >= 0 && fraction.isEmpty())
                || !digits(whole)
                || !digits(fraction)
                || fraction.length() > 9) {
            return null;
        }
        try {
            long scaled = Math.multiplyExact(Long.parseLong(whole), NANO);
            String padded = (fraction + "000000000").substring(0, 9);
            scaled = Math.addExact(scaled, Long.parseLong(padded));
            return negative ? Math.negateExact(scaled) : scaled;
        } catch (ArithmeticException | NumberFormatException error) {
            return null;
        }
    }

    private static boolean digits(String value) {
        for (int index = 0; index < value.length(); index++) {
            if (!Character.isDigit(value.charAt(index))) {
                return false;
            }
        }
        return true;
    }

    public static long routeId(int pickupCell, int dropoffCell) {
        return ((long) pickupCell << ROUTE_SHIFT) | Integer.toUnsignedLong(dropoffCell);
    }

    public static int routePickupCell(long routeId) {
        return (int) (routeId >> ROUTE_SHIFT);
    }

    public static int routeDropoffCell(long routeId) {
        return (int) (routeId & ((1L << ROUTE_SHIFT) - 1L));
    }

    public static String cellText(int cell) {
        return (cell >> CELL_SHIFT) + "." + (cell & CELL_MASK);
    }
}
