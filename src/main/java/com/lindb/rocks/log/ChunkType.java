package com.lindb.rocks.log;

public enum ChunkType {
    ZERO_TYPE((byte) 0),
    FULL((byte) 1),
    FIRST((byte) 2),
    MIDDLE((byte) 3),
    LAST((byte) 4),
    EOF((byte) 5),
    BAD_CHUNK((byte) 6),
    UNKNOWN((byte) 7);

    public static ChunkType getChunkTypeByCode(byte code) {
        switch (code) {
            case 0:
                return ZERO_TYPE;
            case 1:
                return FULL;
            case 2:
                return FIRST;
            case 3:
                return MIDDLE;
            case 4:
                return LAST;
            case 5:
                return EOF;
            case 6:
                return BAD_CHUNK;
            default:
                return UNKNOWN;
        }
    }

    private final byte code;

    ChunkType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }
}
