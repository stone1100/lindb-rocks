package com.lindb.rocks.table;

import com.google.common.collect.PeekingIterator;

import java.util.Map.Entry;

/**
 * @author huang_jie
 *         2/9/2015 12:24 PM
 */
public interface SeekingIterator<K, V> extends PeekingIterator<Entry<K, V>> {
    /**
     * Repositions the iterator so the beginning of this block.
     */
    void seekToFirst();

    /**
     * Repositions the iterator so the key of the next block element returned greater than or equal to the specified targetKey.
     */
    void seek(K targetKey);
}
