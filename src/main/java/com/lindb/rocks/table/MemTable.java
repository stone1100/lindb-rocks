package com.lindb.rocks.table;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.lindb.rocks.LookupKey;
import com.lindb.rocks.LookupResult;
import com.lindb.rocks.util.Bytes;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author huang_jie
 *         2/9/2015 12:54 PM
 */
public class MemTable implements SeekingIterable<InternalKey, byte[]> {
    private final ConcurrentSkipListMap<InternalKey, byte[]> table;
    private final AtomicLong approximateMemoryUsage = new AtomicLong();

    public MemTable(InternalKeyComparator internalKeyComparator) {
        this.table = new ConcurrentSkipListMap<>(internalKeyComparator);
    }

    public boolean isEmpty() {
        return table.isEmpty();
    }

    public long approximateMemoryUsage() {
        return approximateMemoryUsage.get();
    }

    public void add(byte[] key, byte[] value, long timestamp, ValueType valueType) {
        Preconditions.checkNotNull(valueType, "valueType is null");
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(value, "value is null");

        InternalKey internalKey = new InternalKey(key, timestamp, valueType);
        table.put(internalKey, value);

        approximateMemoryUsage.addAndGet(key.length + InternalKey.TIMESTAMP_TYPE_SIZE + value.length);
    }

    public LookupResult get(LookupKey key) {
        Preconditions.checkNotNull(key, "key is null");

        InternalKey internalKey = key.getInternalKey();
        Entry<InternalKey, byte[]> entry = table.ceilingEntry(internalKey);
        if (entry == null) {
            return null;
        }
        InternalKey entryKey = entry.getKey();
        if (Bytes.compareTo(internalKey.getUserKey(), key.getUserKey()) == 0) {
            if (entryKey.getValueType() == ValueType.DELETION) {
                return LookupResult.deleted(key);
            } else {
                return LookupResult.ok(key, entry.getValue());
            }
        }
        return null;
    }

    @Override
    public MemTableIterator iterator() {
        return new MemTableIterator();
    }

    public class MemTableIterator implements InternalIterator {

        private PeekingIterator<Entry<InternalKey, byte[]>> iterator;

        public MemTableIterator() {
            iterator = Iterators.peekingIterator(table.entrySet().iterator());
        }

        @Override
        public void seekToFirst() {
            iterator = Iterators.peekingIterator(table.entrySet().iterator());
        }

        @Override
        public void seek(InternalKey targetKey) {
            iterator = Iterators.peekingIterator(table.tailMap(targetKey).entrySet().iterator());
        }

        @Override
        public InternalEntry peek() {
            return InternalEntry.createEntry(iterator.peek());
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Entry<InternalKey, byte[]> next() {
            return InternalEntry.createEntry(iterator.next());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
