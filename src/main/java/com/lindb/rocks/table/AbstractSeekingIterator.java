package com.lindb.rocks.table;

import java.util.Map.Entry;
import java.util.NoSuchElementException;

/**
 * @author huang_jie
 *         2/9/2015 4:39 PM
 */
public abstract class AbstractSeekingIterator<K, V> implements SeekingIterator<K, V> {
    private Entry<K, V> nextEntry;

    @Override
    public final void seekToFirst() {
        nextEntry = null;
        seekToFirstInternal();
    }

    @Override
    public final void seek(K targetKey) {
        nextEntry = null;
        seekInternal(targetKey);
    }

    @Override
    public final boolean hasNext() {
        if (nextEntry == null) {
            nextEntry = getNextEntry();
        }
        return nextEntry != null;
    }

    @Override
    public final Entry<K, V> next() {
        if (nextEntry == null) {
            nextEntry = getNextEntry();
            if (nextEntry == null) {
                throw new NoSuchElementException();
            }
        }

        Entry<K, V> result = nextEntry;
        nextEntry = null;
        return result;
    }

    @Override
    public final Entry<K, V> peek() {
        if (nextEntry == null) {
            nextEntry = getNextEntry();
            if (nextEntry == null) {
                throw new NoSuchElementException();
            }
        }

        return nextEntry;
    }

    @Override
    public final void remove() {
        throw new UnsupportedOperationException();
    }

    protected abstract void seekToFirstInternal();

    protected abstract void seekInternal(K targetKey);

    protected abstract Entry<K, V> getNextEntry();
}
