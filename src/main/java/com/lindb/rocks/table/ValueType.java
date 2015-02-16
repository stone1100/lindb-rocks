package com.lindb.rocks.table;

/**
 * @author huang_jie
 *         2/9/2015 12:17 PM
 */
public enum ValueType {
    DELETION((byte) 0),
    VALUE((byte) 1);

    public static ValueType getValueTypeByCode(byte code) {
        switch (code) {
            case 0:
                return DELETION;
            case 1:
                return VALUE;
            default:
                throw new IllegalArgumentException("Unknown code " + code);
        }
    }

    private final byte code;

    ValueType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }
}
