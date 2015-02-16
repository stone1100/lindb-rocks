package com.lindb.rocks;

import com.google.common.base.Preconditions;

/**
 * @author huang_jie
 *         2/9/2015 1:10 PM
 */
public class LookupResult {
    public static LookupResult ok(LookupKey key, byte[] value) {
        return new LookupResult(key, value, false);
    }

    public static LookupResult deleted(LookupKey key) {
        return new LookupResult(key, null, true);
    }

    private final LookupKey key;
    private final byte[] value;
    private final boolean deleted;

    private LookupResult(LookupKey key, byte[] value, boolean deleted) {
        Preconditions.checkNotNull(key, "key is null");
        this.key = key;
        if (value != null) {
            this.value = value;
        } else {
            this.value = null;
        }
        this.deleted = deleted;
    }

    public LookupKey getKey() {
        return key;
    }

    public byte[] getValue() {
        if (value == null) {
            return null;
        }
        return value;
    }

    public boolean isDeleted() {
        return deleted;
    }
}
