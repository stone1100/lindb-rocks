package com.lindb.rocks;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map.Entry;

import static com.google.common.collect.Lists.newArrayList;

/**
 * @author huang_jie
 *         2/10/2015 3:44 PM
 */
public class WriteBatch {
    private final List<Entry<byte[], byte[]>> batch = newArrayList();
    private int approximateSize;

    public int getApproximateSize() {
        return approximateSize;
    }

    public int size() {
        return batch.size();
    }

    public WriteBatch put(byte[] key, byte[] value) {
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(value, "value is null");
        batch.add(Maps.immutableEntry(key, value));
        approximateSize += 12 + key.length + value.length;
        return this;
    }

    public WriteBatch delete(byte[] key) {
        Preconditions.checkNotNull(key, "key is null");
        batch.add(Maps.immutableEntry(key, (byte[]) null));
        approximateSize += 6 + key.length;
        return this;
    }

    public void forEach(Handler handler) {
        for (Entry<byte[], byte[]> entry : batch) {
            byte[] key = entry.getKey();
            byte[] value = entry.getValue();
            if (value != null) {
                handler.put(key, value);
            } else {
                handler.delete(key);
            }
        }
    }

    public interface Handler {
        void put(byte[] key, byte[] value);

        void delete(byte[] key);
    }
}
