package com.lindb.rocks.io;

import com.lindb.rocks.CompressionType;

import java.nio.ByteBuffer;

import static com.lindb.rocks.util.Bytes.SIZEOF_BYTE;
import static com.lindb.rocks.util.Bytes.SIZEOF_INT;

public class BlockTrailer {
    public static final int BLOCK_TRAILER_ENCODED_LENGTH = SIZEOF_BYTE + SIZEOF_INT;

    private final CompressionType compressionType;
    private final int crc32c;

    public BlockTrailer(CompressionType compressionType, int crc32c) {
        this.compressionType = compressionType;
        this.crc32c = crc32c;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }

    public int getCrc32c() {
        return crc32c;
    }

    public static BlockTrailer readBlockTrailer(ByteBuffer data) {
        return new BlockTrailer(CompressionType.getCompressionTypeByCode(data.get()), data.getInt());
    }

    public ByteBuffer encode() {
        ByteBuffer data = ByteBuffer.allocate(BLOCK_TRAILER_ENCODED_LENGTH);
        data.put(compressionType.code());
        data.putInt(crc32c);
        return (ByteBuffer) data.flip();
    }
}
