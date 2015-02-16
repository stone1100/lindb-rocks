package com.lindb.rocks.table;

public enum LogChunkType {
    ZERO_TYPE((byte) 0),
    FULL((byte) 1),
    FIRST((byte) 2),
    MIDDLE((byte) 3),
    LAST((byte) 4),
    EOF((byte) 5),
    BAD_CHUNK((byte) 6),
    UNKNOWN((byte) 7);

    public static LogChunkType getLogChunkTypeByCode(byte code) {
        for (LogChunkType logChunkType : LogChunkType.values()) {
            if (logChunkType.code == code) {
                return logChunkType;
            }
        }
        return UNKNOWN;
    }

    private final byte code;

    LogChunkType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }
}
