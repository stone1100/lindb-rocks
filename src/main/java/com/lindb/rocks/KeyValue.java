package com.lindb.rocks;

import com.lindb.rocks.util.Bytes;

/**
 * @author huang_jie
 *         2/2/2015 11:04 AM
 */
public class KeyValue {
    /**
     * Size of the key type field in bytes
     */
    public static final int TYPE_SIZE = Bytes.SIZEOF_BYTE;
    /**
     * Size of the row length field in bytes
     */
    public static final int ROW_LENGTH_SIZE = Bytes.SIZEOF_SHORT;
    /**
     * Size of the timestamp field in bytes
     */
    public static final int TIMESTAMP_SIZE = Bytes.SIZEOF_LONG;
    // Size of the timestamp and type byte on end of a key -- a long + a byte.
    public static final int TIMESTAMP_TYPE_SIZE = TIMESTAMP_SIZE + TYPE_SIZE;

    /**
     * Compare KeyValues.  When we compare KeyValues, we only compare the Key
     * portion.  This means two KeyValues with same Key but different Values are
     * considered the same as far as this Comparator is concerned.
     */
    public static class KVComparator {
        /**
         * Get the b[],o,l for left and right rowkey portions and compare.
         *
         * @param left
         * @param loffset
         * @param llength
         * @param right
         * @param roffset
         * @param rlength
         * @return 0 if equal, <0 if left smaller, >0 if right smaller
         */
        public int compareRows(byte[] left, int loffset, int llength,
                               byte[] right, int roffset, int rlength) {
            return Bytes.compareTo(left, loffset, llength, right, roffset, rlength);
        }

        /**
         * Compares left to right assuming that left,loffset,llength and right,roffset,rlength are
         * full KVs laid out in a flat byte[]s.
         *
         * @param left
         * @param loffset
         * @param llength
         * @param right
         * @param roffset
         * @param rlength
         * @return 0 if equal, <0 if left smaller, >0 if right smaller
         */
        public int compareFlatKey(byte[] left, int loffset, int llength,
                                  byte[] right, int roffset, int rlength) {
            // Compare row
            short lrowlength = Bytes.toShort(left, loffset);
            short rrowlength = Bytes.toShort(right, roffset);
            int compare = compareRows(left, loffset + Bytes.SIZEOF_SHORT,
                    lrowlength, right, roffset + Bytes.SIZEOF_SHORT, rrowlength);
            if (compare != 0) {
                return compare;
            }

            // Compare the rest of the two KVs without making any assumptions about
            // the common prefix. This function will not compare rows anyway, so we
            // don't need to tell it that the common prefix includes the row.
            return compareWithoutRow(0, left, loffset, llength, right, roffset,
                    rlength, rrowlength);
        }

        /**
         * Compare columnFamily, qualifier, timestamp, and key type (everything
         * except the row). This method is used both in the normal comparator and
         * the "same-prefix" comparator. Note that we are assuming that row portions
         * of both KVs have already been parsed and found identical, and we don't
         * validate that assumption here.
         *
         * @param commonPrefix the length of the common prefix of the two key-values being
         *                     compared, including row length and row
         */
        private int compareWithoutRow(int commonPrefix, byte[] left, int loffset,
                                      int llength, byte[] right, int roffset, int rlength, short rowlength) {
            /***
             * KeyValue Format and commonLength:
             * |_keyLen_|_valLen_|_rowLen_|_rowKey_|_Quali_|....
             * ------------------|-------commonLength------|--------------
             */
            int commonLength = ROW_LENGTH_SIZE + rowlength;

            // commonLength + TIMESTAMP_TYPE_SIZE
            int commonLengthWithTSAndType = TIMESTAMP_TYPE_SIZE + commonLength;
            // ColumnFamily + Qualifier length.
            int lcolumnlength = llength - commonLengthWithTSAndType;
            int rcolumnlength = rlength - commonLengthWithTSAndType;

            byte ltype = left[loffset + (llength - 1)];
            byte rtype = right[roffset + (rlength - 1)];

            // If the column is not specified, the "minimum" key type appears the
            // latest in the sorted order, regardless of the timestamp. This is used
            // for specifying the last key/value in a given row, because there is no
            // "lexicographically last column" (it would be infinitely long). The
            // "maximum" key type does not need this behavior.
            if (lcolumnlength == 0 && ltype == Type.Minimum.getCode()) {
                // left is "bigger", i.e. it appears later in the sorted order
                return 1;
            }
            if (rcolumnlength == 0 && rtype == Type.Minimum.getCode()) {
                return -1;
            }

            int lfamilyoffset = commonLength + loffset;
            int rfamilyoffset = commonLength + roffset;

            // Column family length.
            int lfamilylength = left[lfamilyoffset - 1];
            int rfamilylength = right[rfamilyoffset - 1];
            // If left family size is not equal to right family size, we need not
            // compare the qualifiers.
            boolean sameFamilySize = (lfamilylength == rfamilylength);
            int common = 0;
            if (commonPrefix > 0) {
                common = Math.max(0, commonPrefix - commonLength);
                if (!sameFamilySize) {
                    // Common should not be larger than Math.min(lfamilylength,
                    // rfamilylength).
                    common = Math.min(common, Math.min(lfamilylength, rfamilylength));
                } else {
                    common = Math.min(common, Math.min(lcolumnlength, rcolumnlength));
                }
            }
            if (!sameFamilySize) {
                // comparing column family is enough.
                return Bytes.compareTo(left, lfamilyoffset + common, lfamilylength
                        - common, right, rfamilyoffset + common, rfamilylength - common);
            }
            // Compare family & qualifier together.
            final int comparison = Bytes.compareTo(left, lfamilyoffset + common,
                    lcolumnlength - common, right, rfamilyoffset + common,
                    rcolumnlength - common);
            if (comparison != 0) {
                return comparison;
            }

            ////
            // Next compare timestamps.
            long ltimestamp = Bytes.toLong(left,
                    loffset + (llength - TIMESTAMP_TYPE_SIZE));
            long rtimestamp = Bytes.toLong(right,
                    roffset + (rlength - TIMESTAMP_TYPE_SIZE));
            int compare = compareTimestamps(ltimestamp, rtimestamp);
            if (compare != 0) {
                return compare;
            }

            // Compare types. Let the delete types sort ahead of puts; i.e. types
            // of higher numbers sort before those of lesser numbers. Maximum (255)
            // appears ahead of everything, and minimum (0) appears after
            // everything.
            return (0xff & rtype) - (0xff & ltype);
        }

        static int compareTimestamps(final long ltimestamp, final long rtimestamp) {
            // The below older timestamps sorting ahead of newer timestamps looks
            // wrong but it is intentional. This way, newer timestamps are first
            // found when we iterate over a memstore and newer versions are the
            // first we trip over when reading from a store file.
            if (ltimestamp < rtimestamp) {
                return 1;
            } else if (ltimestamp > rtimestamp) {
                return -1;
            }
            return 0;
        }
    }

    /**
     * Key type.
     * Has space for other key types to be added later.  Cannot rely on
     * enum ordinals . They change if item is removed or moved.  Do our own codes.
     */
    public static enum Type {
        Minimum((byte)0),
        Put((byte)4),
        Delete((byte)8),
        DeleteColumn((byte)12),
        // Maximum is used when searching; you look from maximum on down.
        Maximum((byte)255);

        private final byte code;

        Type(final byte c) {
            this.code = c;
        }

        public byte getCode() {
            return this.code;
        }

        /**
         * Cannot rely on enum ordinals . They change if item is removed or moved.
         * Do our own codes.
         * @param b
         * @return Type associated with passed code.
         */
        public static Type codeToType(final byte b) {
            for (Type t : Type.values()) {
                if (t.getCode() == b) {
                    return t;
                }
            }
            throw new RuntimeException("Unknown code " + b);
        }
    }
}
