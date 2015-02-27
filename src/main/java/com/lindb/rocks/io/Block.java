package com.lindb.rocks.io;

import com.google.common.base.Preconditions;
import com.lindb.rocks.table.SeekingIterable;
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
 * Block entry:
 * |size of shared key|size of unshared key|value length|unshared data|value
 *
 * @author huang_jie
 *         2/9/2015 3:31 PM
 */
public class Block {

    public static class Writer {
        private final int blockRestartInterval;
        private final IntVector restartPositions;
        private final Comparator<byte[]> comparator;
        private final ByteArrayStream block;
        private int entryCount;
        private int restartBlockEntryCount;

        private boolean finished;
        private byte[] lastKey;

        public Writer(int blockRestartInterval, Comparator<byte[]> comparator) {
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

    public static class Reader implements SeekingIterable<byte[], byte[]> {
        private final Comparator<byte[]> comparator;
        private ByteBuffer data;
        private ByteBuffer restartPositions;
        private int restartCount;
        private int blockSize;

        public Reader(ByteBuffer block, Comparator<byte[]> comparator) {
            this.comparator = comparator;
            // Keys are prefix compressed.  Every once in a while the prefix compression is restarted and the full key is written.
            // These "restart" locations are written at the end of the file, so you can seek to key without having to read the entire file sequentially.

            // key restart count is the last int of the block
            blockSize = block.remaining();
            block.position(blockSize - SIZEOF_INT);
            restartCount = block.getInt();

            if (restartCount > 0) {
                // restarts are written at the end of the block(<restart positions(int*count of restart)|count of restart>)
                int restartOffset = blockSize - restartCount * SIZEOF_INT - SIZEOF_INT;
                Preconditions.checkArgument(restartOffset < blockSize - SIZEOF_INT, "Block is corrupt: restart offset count is greater than block size");
                block.position(restartOffset);
                block.limit(restartOffset + restartCount * SIZEOF_INT);
                restartPositions = block.slice();

                // data starts at 0 and extends to the restart index
                block.position(0);
                block.limit(restartOffset);
                data = block.slice();
            }
        }

        public int size() {
            return blockSize;
        }

        @Override
        public BlockIterator iterator() {
            return new BlockIterator(data, restartPositions, restartCount, comparator);
        }
    }
}
