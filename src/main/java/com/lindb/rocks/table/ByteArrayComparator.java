package com.lindb.rocks.table;


import com.lindb.rocks.util.Bytes;

public class ByteArrayComparator implements UserComparator {
    @Override
    public String name() {
        return "rocksdb.ByteArrayComparator";
    }

    @Override
    public int compare(byte[] left, byte[] right) {
        return Bytes.compareTo(left, right);
    }

    @Override
    public byte[] findShortestSeparator(byte[] start, byte[] end) {
        // Find length of common prefix
        int sharedBytes = Bytes.calculateSharedBytes(start, end);

        // Do not shorten if one string is a prefix of the other
        if (sharedBytes < Math.min(start.length, end.length)) {
            // if we can add one to the last shared byte without overflow and the two keys differ by more tha one increment at this location.
            int lastSharedByte = Bytes.getUnsignedByte(start, sharedBytes);
            if (lastSharedByte < 0xff && lastSharedByte + 1 < Bytes.getUnsignedByte(end, sharedBytes)) {
                byte[] result = new byte[sharedBytes + 1];
                Bytes.putBytes(result, 0, start, 0, sharedBytes + 1);
                Bytes.putByte(result, sharedBytes, (byte) (lastSharedByte + 1));

                assert (compare(result, end) < 0) : "result must be less than last end";
                return result;
            }
        }
        return start;
    }

    @Override
    public byte[] findShortSuccessor(byte[] key) {
        // Find first character that can be incremented
        for (int i = 0; i < key.length; i++) {
            int b = Bytes.getUnsignedByte(key, i);
            if (b != 0xff) {
                byte[] result = new byte[i + 1];
                Bytes.putBytes(result, 0, key, 0, i + 1);
                Bytes.putByte(result, i, (byte) (b + 1));
                return result;
            }
        }
        // key is a run of 0xffs.  Leave it alone.
        return key;
    }
}
