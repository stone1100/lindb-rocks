package com.lindb.rocks.io;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class BlockMetaTest {

    @Test
    public void testBlockMeta() {
        BlockMeta meta = new BlockMeta(0, 10);
        ByteBuffer byteBuffer = meta.encode();

        BlockMeta meta1 = BlockMeta.readBlockMeta(byteBuffer.array());
        Assert.assertEquals(meta.getOffset(), meta1.getOffset());
        Assert.assertEquals(meta.getDataSize(), meta1.getDataSize());
    }
}