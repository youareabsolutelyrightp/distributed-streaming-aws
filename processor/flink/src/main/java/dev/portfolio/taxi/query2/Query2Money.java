package dev.portfolio.taxi.query2;

public final class Query2Money {
    private Query2Money() {}

    public static Long farePlusTipCents(String fareValue, String tipValue) {
        Long fare = parseCents(fareValue);
        Long tip = parseCents(tipValue);
        if (fare == null || tip == null || fare < 0L || tip < 0L) {
            return null;
        }
        try {
            return Math.addExact(fare, tip);
        } catch (ArithmeticException overflow) {
            return null;
        }
    }

    static Long parseCents(String value) {
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
        if (whole.isEmpty() || (separator >= 0 && fraction.isEmpty()) || fraction.length() > 2
                || !digits(whole) || !digits(fraction)) {
            return null;
        }
        try {
            long cents = Math.multiplyExact(Long.parseLong(whole), 100L);
            cents = Math.addExact(cents, Long.parseLong((fraction + "00").substring(0, 2)));
            return negative ? Math.negateExact(cents) : cents;
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
}
