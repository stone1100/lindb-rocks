package com.lindb.rocks;

public enum CompressionType {
    NONE((byte) 0),
    SNAPPY((byte) 1);

    public static CompressionType getCompressionTypeByCode(byte code) {
        for (CompressionType compressionType : CompressionType.values()) {
            if (compressionType.code == code) {
                return compressionType;
            }
        }
        throw new IllegalArgumentException("Unknown code " + code);
    }

    private final byte code;

    CompressionType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }
}
