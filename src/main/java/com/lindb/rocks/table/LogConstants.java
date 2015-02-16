package com.lindb.rocks.table;

import static com.lindb.rocks.util.Bytes.*;

public interface LogConstants {
    public static final int BLOCK_SIZE = 32768;
    //Chunk Header is checksum (4 bytes), type (1 byte), length (2 bytes).
    public static final int CHUNK_HEADER_SIZE = SIZEOF_INT + SIZEOF_BYTE + SIZEOF_SHORT;
    //Record header is sequence number(8 bytes) + record count(4b bytes)
    public static final int RECORD_HEADER_SIZE = SIZEOF_LONG + SIZEOF_INT;
}
