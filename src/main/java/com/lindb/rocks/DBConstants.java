package com.lindb.rocks;

import com.lindb.rocks.util.Bytes;

public interface DBConstants {
    /**
     * An empty instance.
     */
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    //Record header is sequence number(8 bytes) + record count(4b bytes)
    public static final int RECORD_HEADER_SIZE = Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT;
    // We leave eight bits empty at the bottom so a type and sequence# can be packed together into 64-bits.
    public static final long MAX_SEQUENCE_NUMBER = ((0x1L << 56) - 1);
    public static final int MAJOR_VERSION = 0;
    public static final int MINOR_VERSION = 1;
    public static final int ALLOW_SEEK_SIZE = 16 * 1024;//16KB

    // todo this should be part of the configuration

    /**
     * Max number of levels
     */
    public static final int NUM_LEVELS = 7;

    /**
     * Level-0 compaction is started when we hit this many files.
     */
    public static final int L0_COMPACTION_TRIGGER = 4;

    /**
     * Soft limit on number of level-0 files.  We slow down writes at this point.
     */
    public static final int L0_SLOWDOWN_WRITES_TRIGGER = 8;

    /**
     * Maximum number of level-0 files.  We stop writes at this point.
     */
    public static final int L0_STOP_WRITES_TRIGGER = 12;

    /**
     * Maximum level to which a new compacted memtable is pushed if it
     * does not create overlap.  We try to push to level 2 to avoid the
     * relatively expensive level 0=>1 compactions and to avoid some
     * expensive manifest file operations.  We do not push all the way to
     * the largest level since that can generate a lot of wasted disk
     * space if the same key space is being repeatedly overwritten.
     */
    public static final int MAX_MEM_COMPACT_LEVEL = 2;
}
