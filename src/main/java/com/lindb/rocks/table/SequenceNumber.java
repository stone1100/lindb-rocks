package com.lindb.rocks.table;

/**
 * @author huang_jie
 *         2/9/2015 12:48 PM
 */
public final class SequenceNumber {
    // We leave eight bits empty at the bottom so a type and sequence#
    // can be packed together into 64-bits.
    public static final long MAX_SEQUENCE_NUMBER = ((0x1L << 56) - 1);

    private SequenceNumber() {
    }

}
