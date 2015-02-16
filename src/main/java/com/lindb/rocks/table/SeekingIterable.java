package com.lindb.rocks.table;

import java.util.Map.Entry;

/**
 * @author huang_jie
 *         2/9/2015 12:27 PM
 */
public interface SeekingIterable<K, V> extends Iterable<Entry<K, V>> {
    @Override
    SeekingIterator<K, V> iterator();
}
