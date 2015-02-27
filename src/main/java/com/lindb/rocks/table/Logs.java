package com.lindb.rocks.table;

import com.lindb.rocks.util.PureJavaCrc32C;

import java.nio.ByteBuffer;

public final class Logs {
    private Logs() {
    }

    public static int getChunkChecksum(int chunkTypeId, ByteBuffer chunk) {
        return getChunkChecksum(chunkTypeId, chunk.array(), chunk.position(), chunk.remaining());
    }

    public static int getChunkChecksum(int chunkTypeId, byte[] buffer, int offset, int length) {
        // Compute the crc of the record type and the payload.
        PureJavaCrc32C crc32C = new PureJavaCrc32C();
        crc32C.update(chunkTypeId);
        crc32C.update(buffer, offset, length);
        return crc32C.getMaskedValue();
    }
}
