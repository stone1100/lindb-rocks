package com.lindb.rocks.io;

import com.lindb.rocks.CompressionType;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class BlockTrailerTest {

    @Test
    public void testBlockTrailer() {
        BlockTrailer trailer = new BlockTrailer(CompressionType.SNAPPY, 32);
        ByteBuffer data = trailer.encode();
        BlockTrailer trailer1 = BlockTrailer.readBlockTrailer(data);
        Assert.assertEquals(trailer.getCompressionType(), trailer1.getCompressionType());
        Assert.assertEquals(trailer.getCrc32c(), trailer1.getCrc32c());
    }
}