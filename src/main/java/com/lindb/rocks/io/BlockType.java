package com.lindb.rocks.io;

/**
 * @author huang_jie
 *         2/13/2015 5:17 PM
 */
public enum BlockType {

    DATA((byte) 1);

    private final byte code;

    BlockType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }
}
