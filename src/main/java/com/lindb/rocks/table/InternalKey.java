package com.lindb.rocks.table;

import com.google.common.base.Preconditions;
import com.lindb.rocks.util.Bytes;

import java.util.Arrays;

/**
 * @author huang_jie
 *         2/9/2015 12:38 PM
 */
public class InternalKey {
    /**
     * Size of the key type field in bytes
     */
    public static final int TYPE_SIZE = Bytes.SIZEOF_BYTE;
    /**
     * Size of the timestamp field in bytes
     */
    public static final int TIMESTAMP_SIZE = Bytes.SIZEOF_LONG;
    /**
     * Size of the timestamp and type byte on end of a key -- a long + a byte.
     */
    public static final int TIMESTAMP_TYPE_SIZE = TIMESTAMP_SIZE + TYPE_SIZE;

    private final byte[] userKey;
    private final long timestamp;
    private final ValueType valueType;

    public InternalKey(byte[] userKey, ValueType valueType) {
        //TODO modify default timestamp
        this(userKey, System.currentTimeMillis(), valueType);
    }

    public InternalKey(byte[] userKey, long timestamp, ValueType valueType) {
        Preconditions.checkNotNull(userKey, "userKey is null");
        Preconditions.checkNotNull(valueType, "valueType is null");

        this.userKey = userKey;
        this.timestamp = timestamp;
        this.valueType = valueType;
    }

    public InternalKey(byte[] data) {
        Preconditions.checkNotNull(data, "data is null");
        Preconditions.checkArgument(data.length >= TIMESTAMP_TYPE_SIZE, "data must be at least %s bytes.(timestamp<long> + value type<byte>", TIMESTAMP_TYPE_SIZE);

        this.userKey = getUserKey(data);
        int length = data.length;
        this.timestamp = Bytes.toLong(data, length - TIMESTAMP_TYPE_SIZE);
        this.valueType = ValueType.getValueTypeByCode(data[length - 1]);
    }

    public byte[] getUserKey() {
        return userKey;
    }

    public ValueType getValueType() {
        return valueType;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public byte[] encode() {
        byte[] data = new byte[userKey.length + TIMESTAMP_TYPE_SIZE];
        int offset = Bytes.putBytes(data, 0, userKey, 0, userKey.length);
        offset = Bytes.putLong(data, offset, timestamp);
        Bytes.putByte(data, offset, valueType.code());
        return data;
    }

    private byte[] getUserKey(byte[] data) {
        return Bytes.sub(data, 0, data.length - TIMESTAMP_TYPE_SIZE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InternalKey that = (InternalKey) o;

        if (timestamp != that.timestamp) return false;
        if (!Arrays.equals(userKey, that.userKey)) return false;
        if (valueType != that.valueType) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = userKey != null ? Arrays.hashCode(userKey) : 0;
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (valueType != null ? valueType.hashCode() : 0);
        return result;
    }
}
