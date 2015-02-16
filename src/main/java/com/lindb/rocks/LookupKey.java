package com.lindb.rocks;

import com.lindb.rocks.table.InternalKey;
import com.lindb.rocks.table.ValueType;

/**
 * @author huang_jie
 *         2/9/2015 1:10 PM
 */
public class LookupKey {
    private final InternalKey key;

    public LookupKey(byte[] userKey, long sequenceNumber) {
        key = new InternalKey(userKey, sequenceNumber, ValueType.VALUE);
    }

    public InternalKey getInternalKey() {
        return key;
    }

    public byte[] getUserKey() {
        return key.getUserKey();
    }

    @Override
    public String toString() {
        return "LookupKey{" +
                "key=" + key +
                '}';
    }
}
