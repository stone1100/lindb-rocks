package com.lindb.rocks.util;

import java.util.Locale;

/**
 * @author huang_jie
 *         2/2/2015 4:39 PM
 */
public class StringUtils {
    public static String humanReadableInt(long number) {
        return TraditionalBinaryPrefix.long2String(number, "", 1);
    }

    /**
     * The same as String.format(Locale.ENGLISH, format, objects).
     */
    public static String format(final String format, final Object... objects) {
        return String.format(Locale.ENGLISH, format, objects);
    }

    public static enum TraditionalBinaryPrefix {
        KILO(10),
        MEGA(KILO.bitShift + 10),
        GIGA(MEGA.bitShift + 10),
        TERA(GIGA.bitShift + 10),
        PETA(TERA.bitShift + 10),
        EXA(PETA.bitShift + 10);

        public final long value;
        public final char symbol;
        public final int bitShift;
        public final long bitMask;

        private TraditionalBinaryPrefix(int bitShift) {
            this.bitShift = bitShift;
            this.value = 1L << bitShift;
            this.bitMask = this.value - 1L;
            this.symbol = toString().charAt(0);
        }

        /**
         * @return The TraditionalBinaryPrefix object corresponding to the symbol.
         */
        public static TraditionalBinaryPrefix valueOf(char symbol) {
            symbol = Character.toUpperCase(symbol);
            for (TraditionalBinaryPrefix prefix : TraditionalBinaryPrefix.values()) {
                if (symbol == prefix.symbol) {
                    return prefix;
                }
            }
            throw new IllegalArgumentException("Unknown symbol '" + symbol + "'");
        }

        /**
         * Convert a string to long.
         * The input string is first be trimmed
         * and then it is parsed with traditional binary prefix.
         * <p/>
         * For example,
         * "-1230k" will be converted to -1230 * 1024 = -1259520;
         * "891g" will be converted to 891 * 1024^3 = 956703965184;
         *
         * @param s input string
         * @return a long value represented by the input string.
         */
        public static long string2long(String s) {
            s = s.trim();
            final int lastpos = s.length() - 1;
            final char lastchar = s.charAt(lastpos);
            if (Character.isDigit(lastchar))
                return Long.parseLong(s);
            else {
                long prefix;
                try {
                    prefix = TraditionalBinaryPrefix.valueOf(lastchar).value;
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Invalid size prefix '" + lastchar
                            + "' in '" + s
                            + "'. Allowed prefixes are k, m, g, t, p, e(case insensitive)");
                }
                long num = Long.parseLong(s.substring(0, lastpos));
                if (num > (Long.MAX_VALUE / prefix) || num < (Long.MIN_VALUE / prefix)) {
                    throw new IllegalArgumentException(s + " does not fit in a Long");
                }
                return num * prefix;
            }
        }

        /**
         * Convert a long integer to a string with traditional binary prefix.
         *
         * @param n             the value to be converted
         * @param unit          The unit, e.g. "B" for bytes.
         * @param decimalPlaces The number of decimal places.
         * @return a string with traditional binary prefix.
         */
        public static String long2String(long n, String unit, int decimalPlaces) {
            if (unit == null) {
                unit = "";
            }
            //take care a special case
            if (n == Long.MIN_VALUE) {
                return "-8 " + EXA.symbol + unit;
            }

            final StringBuilder b = new StringBuilder();
            //take care negative numbers
            if (n < 0) {
                b.append('-');
                n = -n;
            }
            if (n < KILO.value) {
                //no prefix
                b.append(n);
                return (unit.isEmpty() ? b : b.append(" ").append(unit)).toString();
            } else {
                //find traditional binary prefix
                int i = 0;
                for (; i < values().length && n >= values()[i].value; i++) ;
                TraditionalBinaryPrefix prefix = values()[i - 1];

                if ((n & prefix.bitMask) == 0) {
                    //exact division
                    b.append(n >> prefix.bitShift);
                } else {
                    final String format = "%." + decimalPlaces + "f";
                    String s = format(format, n / (double) prefix.value);
                    //check a special rounding up case
                    if (s.startsWith("1024")) {
                        prefix = values()[i];
                        s = format(format, n / (double) prefix.value);
                    }
                    b.append(s);
                }
                return b.append(' ').append(prefix.symbol).append(unit).toString();
            }
        }
    }
}
