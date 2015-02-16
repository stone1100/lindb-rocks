package com.lindb.rocks.io;

import com.google.common.base.Preconditions;
import com.lindb.rocks.table.SeekingIterable;

import java.nio.ByteBuffer;
import java.util.Comparator;

import static com.lindb.rocks.util.Bytes.SIZEOF_INT;

/**
 * Block structure:
 * |block entry 1|block entry 2|....|block entry N|restart positions(int*count of restart)|count of restart
 * <p/>
 * Block entry:
 * |size of shared key|size of unshared key|value length|unshared data|value
 *
 * @author huang_jie
 *         2/9/2015 3:31 PM
 */
public class Block implements SeekingIterable<byte[], byte[]> {
    private final Comparator<byte[]> comparator;
    private ByteBuffer data;
    private ByteBuffer restartPositions;
    private int restartCount;
    private int blockSize;

    public Block(ByteBuffer block, Comparator<byte[]> comparator) {
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
