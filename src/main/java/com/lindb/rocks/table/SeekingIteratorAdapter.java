package com.lindb.rocks.table;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

public class SeekingIteratorAdapter implements Iterator<Entry<byte[], byte[]>>, Closeable {
    private final SnapshotSeekingIterator seekingIterator;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public SeekingIteratorAdapter(SnapshotSeekingIterator seekingIterator) {
        this.seekingIterator = seekingIterator;
    }

    public void seekToFirst() {
        seekingIterator.seekToFirst();
    }

    public void seek(byte[] targetKey) {
        seekingIterator.seek(targetKey);
    }

    @Override
    public boolean hasNext() {
        return seekingIterator.hasNext();
    }

    @Override
    public DBEntry next() {
        return adapt(seekingIterator.next());
    }

    public DBEntry peekNext() {
        return adapt(seekingIterator.peek());
    }

    @Override
    public void close() {
        // This is an end user API.. he might screw up and close multiple times.
        // but we don't want the close multiple times as reference counts go bad.
        if (closed.compareAndSet(false, true)) {
            seekingIterator.close();
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    private DBEntry adapt(Entry<byte[], byte[]> entry) {
        return new DBEntry(entry.getKey(), entry.getValue());
    }


    public static class DBEntry implements Entry<byte[], byte[]> {
        private final byte[] key;
        private final byte[] value;

        public DBEntry(byte[] key, byte[] value) {
            Preconditions.checkNotNull(key, "key is null");
            Preconditions.checkNotNull(value, "value is null");
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

    }
}
