package com.lindb.rocks.table;

import com.google.common.collect.Maps;
import com.lindb.rocks.DBIterator;

import java.util.Comparator;
import java.util.Map.Entry;

public final class SnapshotSeekingIterator extends AbstractSeekingIterator<byte[], byte[]> {
    private final DBIterator iterator;
    private final Snapshot snapshot;
    private final Comparator<byte[]> userComparator;

    public SnapshotSeekingIterator(DBIterator iterator, Snapshot snapshot, Comparator<byte[]> userComparator) {
        this.iterator = iterator;
        this.snapshot = snapshot;
        this.userComparator = userComparator;
        this.snapshot.getVersion().retain();
    }

    public void close() {
        this.snapshot.getVersion().release();
    }

    @Override
    protected void seekToFirstInternal() {
        iterator.seekToFirst();
        findNextUserEntry(null);
    }

    @Override
    protected void seekInternal(byte[] targetKey) {
        iterator.seek(new InternalKey(targetKey, snapshot.getLastSequence(), ValueType.VALUE));
        findNextUserEntry(null);
    }

    @Override
    protected Entry<byte[], byte[]> getNextEntry() {
        if (!iterator.hasNext()) {
            return null;
        }

        Entry<InternalKey, byte[]> next = iterator.next();

        // find the next user entry after the key we are about to return
        findNextUserEntry(next.getKey().getUserKey());

        return Maps.immutableEntry(next.getKey().getUserKey(), next.getValue());
    }

    private void findNextUserEntry(byte[] deletedKey) {
        // if there are no more entries, we are done
        if (!iterator.hasNext()) {
            return;
        }

        do {
            // Peek the next entry and parse the key
            InternalKey internalKey = iterator.peek().getKey();

            // skip entries created after our snapshot
            if (internalKey.getTimestamp() > snapshot.getLastSequence()) {
                iterator.next();
                continue;
            }

            // if the next entry is a deletion, skip all subsequent entries for that key
            if (internalKey.getValueType() == ValueType.DELETION) {
                deletedKey = internalKey.getUserKey();
            } else if (internalKey.getValueType() == ValueType.VALUE) {
                // is this value masked by a prior deletion record?
                if (deletedKey == null || userComparator.compare(internalKey.getUserKey(), deletedKey) > 0) {
                    return;
                }
            }
            iterator.next();
        } while (iterator.hasNext());
    }

    @Override
    public String toString() {
        return "SnapshotSeekingIterator{" +
                "snapshot=" + snapshot +
                ", iterator=" + iterator +
                '}';
    }
}
