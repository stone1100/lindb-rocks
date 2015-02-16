package com.lindb.rocks.table;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Map.Entry;

/**
 * @author huang_jie
 *         2/9/2015 1:26 PM
 */
public class InternalEntry implements Entry<InternalKey, byte[]> {
    private final InternalKey key;
    private final byte[] value;

    public InternalEntry(InternalKey key, byte[] value) {
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(value, "value is null");
        this.key = key;
        this.value = value;
    }

    @Override
    public InternalKey getKey() {
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

    public static InternalEntry createEntry(Entry<InternalKey, byte[]> entry) {
        return new InternalEntry(entry.getKey(), entry.getValue());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InternalEntry that = (InternalEntry) o;

        if (key != null ? !key.equals(that.key) : that.key != null) return false;
        if (!Arrays.equals(value, that.value)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? Arrays.hashCode(value) : 0);
        return result;
    }

    @Override
    public String toString() {
        return "InternalEntry{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }
}
