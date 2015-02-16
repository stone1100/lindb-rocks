package com.lindb.rocks.io;

import com.google.common.base.Preconditions;
import com.lindb.rocks.table.SeekingIterator;
import com.lindb.rocks.util.Bytes;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.NoSuchElementException;

/**
 * @author huang_jie
 *         2/9/2015 3:37 PM
 */
public class BlockIterator implements SeekingIterator<byte[], byte[]> {
    private final ByteBuffer data;
    private final ByteBuffer restartPositions;
    private final int restartCount;
    private final Comparator<byte[]> comparator;

    private BlockEntry nextEntry;

    public BlockIterator(ByteBuffer data, ByteBuffer restartPositions, int restartCount, Comparator<byte[]> comparator) {
        this.data = data;
        this.restartPositions = restartPositions;
        this.restartCount = restartCount;
        this.comparator = comparator;

        seekToFirst();
    }

    @Override
    public void seekToFirst() {
        if (restartCount > 0) {
            seekToRestartPosition(0);
        }
    }

    /**
     * Repositions the iterator so the key of the next BlockElement returned greater than or equal to the specified targetKey.
     */
    @Override
    public void seek(byte[] targetKey) {
        if (restartCount <= 0) {
            return;
        }
        int left = 0;
        int right = restartCount - 1;
        // binary search restart positions to find the restart position immediately before the targetKey
        while (left < right) {
            int mid = (left + right + 1) / 2;
            seekToRestartPosition(mid);

            if (comparator.compare(nextEntry.getKey(), targetKey) < 0) {
                //key at mid is smaller than targetKey. Therefore all restart blocks before mid are uninteresting.
                left = mid;
            } else {
                // key at mid is greater than or equal to targetKey. Therefore all restart blocks at or after mid are uninteresting.
                right = mid - 1;
            }
        }

        // linear search (within restart block) for first key greater than or equal to targetKey
        for (seekToRestartPosition(left); nextEntry != null; next()) {
            if (comparator.compare(peek().getKey(), targetKey) >= 0) {
                break;
            }
        }
    }

    @Override
    public BlockEntry peek() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return nextEntry;
    }

    @Override
    public boolean hasNext() {
        return nextEntry != null;
    }

    @Override
    public BlockEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        BlockEntry entry = nextEntry;
        if (data.remaining() == 0) {
            nextEntry = null;
        } else {
            // read entry at current data position
            nextEntry = readEntry(data, nextEntry.getKey());
        }
        return entry;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /**
     * Seeks to and reads the entry at the specified restart position.
     * <p/>
     * After this method, nextEntry will contain the next entry to return, and the previousEntry will be null.
     */
    private void seekToRestartPosition(int restartPosition) {
        // seek data readIndex to the beginning of the restart block
        int offset = restartPositions.getInt(restartPosition * Bytes.SIZEOF_INT);
        data.position(offset);

        // clear the entries to assure key is not prefixed
        nextEntry = null;

        // read the entry
        nextEntry = readEntry(data, null);
    }

    /**
     * Reads the entry at the current data readIndex.
     * After this method, data readIndex is positioned at the beginning of the next entry
     * or at the end of data if there was not a next entry.
     *
     * @return true if an entry was read
     */
    private static BlockEntry readEntry(ByteBuffer data, byte[] previousKey) {
        Preconditions.checkNotNull(data, "data is null");

        // read entry header
        int sharedKeyLength = data.getInt();
        int nonSharedKeyLength = data.getInt();
        int valueLength = data.getInt();

        // read key
        byte[] key = new byte[sharedKeyLength + nonSharedKeyLength];
        if (sharedKeyLength > 0) {
            assert previousKey != null : "Entry has a shared key but no previous entry was provided";
            System.arraycopy(previousKey, 0, key, 0, sharedKeyLength);
        }
        data.get(key, sharedKeyLength, nonSharedKeyLength);

        // read value
        byte[] value = new byte[valueLength];
        data.get(value);
        return new BlockEntry(key, value);
    }
}
