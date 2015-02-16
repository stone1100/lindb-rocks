package com.lindb.rocks.table;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @author huang_jie
 *         2/10/2015 12:21 PM
 */
public class InternalTableIterator extends AbstractSeekingIterator<InternalKey, byte[]> implements InternalIterator {
    private final TableIterator tableIterator;

    public InternalTableIterator(TableIterator tableIterator) {
        this.tableIterator = tableIterator;
    }

    @Override
    protected void seekToFirstInternal() {
        tableIterator.seekToFirstInternal();
    }

    @Override
    protected void seekInternal(InternalKey targetKey) {
        tableIterator.seek(targetKey.encode());
    }

    @Override
    protected Map.Entry<InternalKey, byte[]> getNextEntry() {
        if (tableIterator.hasNext()) {
            Map.Entry<byte[], byte[]> next = tableIterator.next();
            return Maps.immutableEntry(new InternalKey(next.getKey()), next.getValue());
        }
        return null;
    }
}
