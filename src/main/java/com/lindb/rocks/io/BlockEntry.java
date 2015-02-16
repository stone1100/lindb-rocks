package com.lindb.rocks.io;

import java.util.Arrays;
import java.util.Map.Entry;

/**
 * @author huang_jie
 *         2/9/2015 3:39 PM
 */
public class BlockEntry implements Entry<byte[], byte[]> {
    private final byte[] key;
    private final byte[] value;

    public BlockEntry(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public byte[] setValue(byte[] value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BlockEntry entry = (BlockEntry) o;

        if (!Arrays.equals(key, entry.key)) return false;
        if (!Arrays.equals(value, entry.value)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = key != null ? Arrays.hashCode(key) : 0;
        result = 31 * result + (value != null ? Arrays.hashCode(value) : 0);
        return result;
    }
}
