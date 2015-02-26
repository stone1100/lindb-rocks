package com.lindb.rocks.util;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SnappyTest {
    private final static String text = "If the infrastructure starves on disk capacity but has no performance problems it may be logical to use an algorithm that give huge compression ratios, losing some time on CPU but that’s usually not the case. Large capacity disks are far cheaper than fast storage solutions (think SSDs) so it is better for a compression algorithm being faster than being able to give higher compression ratios. Because of that hadoop applications prefer LZO, a real-time fast compression library, to ZLIB variants. Of course these are general talks and to see real performance changes and compression ratios, one have to try those algorithms with his/her own data";

    @Test
    public void testCompress() throws Exception {
        byte[] data = Bytes.toBytes(text);
        byte[] compressData = new byte[data.length];
        int compressedSize = Snappy.compress(data, 0, data.length, compressData, 0);

        ByteBuffer uncompressedBuffer = ByteBuffer.allocateDirect(compressedSize);
        uncompressedBuffer.put(compressData, 0, compressedSize);
        uncompressedBuffer.flip();

        int uncompressedLength = Bytes.readVariableLengthInt(uncompressedBuffer.duplicate());

        ByteBuffer uncompressedScratch = ByteBuffer.allocateDirect(uncompressedLength);

        Snappy.uncompress(uncompressedBuffer, uncompressedScratch);

        byte[] temp = new byte[uncompressedLength];
        uncompressedScratch.get(temp);

        String result = Bytes.toString(temp);
        assert result.equals(text);
    }

    @Test
    public void testCompressInt() throws IOException {
        int value = -99;
        byte[] data = Bytes.toBytes(value);
        byte[] compressData = new byte[data.length * 2];
        int compressedSize = Snappy.compress(data, 0, data.length, compressData, 0);

        ByteBuffer uncompressedBuffer = ByteBuffer.allocateDirect(compressedSize);
        uncompressedBuffer.put(compressData, 0, compressedSize);
        uncompressedBuffer.flip();

        int uncompressedLength = Bytes.readVariableLengthInt(uncompressedBuffer.duplicate());

        ByteBuffer uncompressedScratch = ByteBuffer.allocateDirect(uncompressedLength);

        Snappy.uncompress(uncompressedBuffer, uncompressedScratch);

        byte[] temp = new byte[uncompressedLength];
        uncompressedScratch.get(temp);

        int result = Bytes.toInt(temp);
        assert result == value;
    }

    @Test
    public void testCompressIntAndLong() throws IOException {
        long offset = 0;
        int dataSize = 79;
        ByteBuffer t = ByteBuffer.allocate(Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT);
        t.putLong(offset);
        t.putInt(dataSize);
        byte[] data = t.array();
        byte[] compressData = new byte[data.length * 2];
        int compressedSize = Snappy.compress(data, 0, data.length, compressData, 0);

        ByteBuffer uncompressedBuffer = ByteBuffer.allocateDirect(compressedSize);
        uncompressedBuffer.put(compressData, 0, compressedSize);
        uncompressedBuffer.flip();

        int uncompressedLength = Bytes.readVariableLengthInt(uncompressedBuffer.duplicate());

        ByteBuffer uncompressedScratch = ByteBuffer.allocateDirect(uncompressedLength);

        Snappy.uncompress(uncompressedBuffer, uncompressedScratch);

        byte[] temp = new byte[uncompressedLength];
        uncompressedScratch.get(temp);

        long rOffset = Bytes.toLong(temp);
        long rDataSize = Bytes.toInt(temp, 8, 4);
        assert rOffset == offset;
        assert rDataSize == dataSize;
    }

}