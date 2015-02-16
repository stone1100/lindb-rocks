package com.lindb.rocks.io;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class FooterTest {
    @Test
    public void testFooter() {
        Footer footer = new Footer(new BlockMeta(0, 10), new BlockMeta(10, 5));
        ByteBuffer byteBuffer = footer.encode();
        Footer footer1 = Footer.readFooter(byteBuffer);
        Assert.assertEquals(footer.getIndexBlockMeta().getOffset(), footer1.getIndexBlockMeta().getOffset());
        Assert.assertEquals(footer.getIndexBlockMeta().getDataSize(),footer1.getIndexBlockMeta().getDataSize());

        Assert.assertEquals(footer.getMetaIndexBlockMeta().getOffset(), footer1.getMetaIndexBlockMeta().getOffset());
        Assert.assertEquals(footer.getMetaIndexBlockMeta().getDataSize(),footer1.getMetaIndexBlockMeta().getDataSize());
    }

}