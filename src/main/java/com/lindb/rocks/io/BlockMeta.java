package com.lindb.rocks.io;

import com.lindb.rocks.util.Bytes;

import java.nio.ByteBuffer;

import static com.lindb.rocks.util.Bytes.SIZEOF_INT;
import static com.lindb.rocks.util.Bytes.SIZEOF_LONG;

/**
 * @author huang_jie
 *         2/9/2015 4:08 PM
 */
public class BlockMeta {
    public static final int BLOCK_META_ENCODED_LENGTH = SIZEOF_LONG + SIZEOF_INT;
    private final long offset;
    private final int dataSize;

    public BlockMeta(long offset, int dataSize) {
        this.offset = offset;
        this.dataSize = dataSize;
    }

    public long getOffset() {
        return offset;
    }

    public int getDataSize() {
        return dataSize;
    }

    public static BlockMeta readBlockMeta(byte[] data) {
        return new BlockMeta(Bytes.toLong(data), Bytes.toInt(data, SIZEOF_LONG));
    }

    public ByteBuffer encode() {
        ByteBuffer data = ByteBuffer.allocate(BLOCK_META_ENCODED_LENGTH);
        data.putLong(offset);
        data.putInt(dataSize);
        return (ByteBuffer)data.flip();
    }

}
