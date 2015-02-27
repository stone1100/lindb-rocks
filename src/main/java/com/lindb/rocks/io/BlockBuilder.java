package com.lindb.rocks.io;

import com.google.common.base.Preconditions;
import com.lindb.rocks.util.Bytes;
import com.lindb.rocks.util.IntVector;

import java.nio.ByteBuffer;
import java.util.Comparator;

import static com.lindb.rocks.util.Bytes.SIZEOF_INT;

/**
 * Block is sstable:
 * block entry 1|block entry 2|....|block entry N|restart positions(int*count of restart)|count of restart|trailer(compression type+crc32)
 * <p/>
 * Restart point must be start with 0
 */
public class BlockBuilder {
    private final int blockRestartInterval;
    private final IntVector restartPositions;
    private final Comparator<byte[]> comparator;
    private final ByteArrayStream block;
    private int entryCount;
    private int restartBlockEntryCount;

    private boolean finished;
    private byte[] lastKey;

    public BlockBuilder(int blockRestartInterval, Comparator<byte[]> comparator) {
        Preconditions.checkArgument(blockRestartInterval >= 0, "blockRestartInterval is negative");

        this.block = new ByteArrayStream();
        this.blockRestartInterval = blockRestartInterval;
        this.comparator = comparator;

        restartPositions = new IntVector(32);
        restartPositions.add(0);  // first restart point must be 0
    }

    public void reset() {
        block.reset();
        entryCount = 0;
        restartPositions.clear();
        restartPositions.add(0); // first restart point must be 0
        restartBlockEntryCount = 0;
        lastKey = null;
        finished = false;
    }

    public boolean isEmpty() {
        return entryCount == 0;
    }

    public int currentSizeEstimate() {
        // no need to estimate if closed
        if (finished) {
            return block.size();
        }

        // no records is just a single int
        if (block.size() == 0) {
            return SIZEOF_INT;
        }
        //raw data size + restart position count+ pre restart position*count
        return block.size() + SIZEOF_INT + SIZEOF_INT * restartPositions.size();
    }

    public void add(byte[] key, byte[] value) {
        Preconditions.checkState(!finished, "block is finished");
        Preconditions.checkPositionIndex(restartBlockEntryCount, blockRestartInterval);
        Preconditions.checkArgument(lastKey == null || comparator.compare(key, lastKey) > 0, "key must be greater than last key");

        int sharedKeyBytes = 0;
        if (restartBlockEntryCount < blockRestartInterval) {
            sharedKeyBytes = Bytes.calculateSharedBytes(key, lastKey);
        } else {
            // restart prefix compression
            restartPositions.add(block.size());
            restartBlockEntryCount = 0;
        }

        int nonSharedKeyBytes = key.length - sharedKeyBytes;

        // write "<shared><non_shared><value_size>"
        block.write(sharedKeyBytes);
        block.write(nonSharedKeyBytes);
        block.write(value.length);

        // write non-shared key bytes
        block.write(key, sharedKeyBytes, nonSharedKeyBytes);

        // write value bytes
        block.write(value);

        // update last key
        lastKey = key;

        // update state
        entryCount++;
        restartBlockEntryCount++;
    }

    public ByteBuffer finish() {
        if (!finished) {
            finished = true;
            if (entryCount > 0) {
                restartPositions.write(block);
                block.write(restartPositions.size());
            } else {
                block.write(0);
            }
        }
        return ByteBuffer.wrap(block.getBuf(), 0, block.size());
    }
}
