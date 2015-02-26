package com.lindb.rocks.table;

import com.google.common.collect.ImmutableList;
import com.lindb.rocks.util.CloseableUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import static com.google.common.base.Charsets.UTF_8;
import static java.util.Arrays.asList;

/**
 * @author huang_jie
 *         2/26/2015 4:44 PM
 */
public class LogTest {
    private static final LogMonitor NO_CORRUPTION_MONITOR = new LogMonitor() {
        @Override
        public void corruption(long bytes, String reason) {
            Assert.fail(String.format("corruption of %s bytes: %s", bytes, reason));
        }

        @Override
        public void corruption(long bytes, Throwable reason) {
            throw new RuntimeException(String.format("corruption of %s bytes: %s", bytes, reason), reason);
        }
    };

    private MMapLogWriter writer;

    @Test
    public void testEmptyBlock() throws Exception {
        testLog();
    }

    @Test
    public void testSmallRecord() throws Exception {
        testLog(toBuffer("dain sundstrom"));
    }

    @Test
    public void testMultipleSmallRecords() throws Exception {
        List<ByteBuffer> records = asList(
                toBuffer("Lagunitas  Little Sumpin’ Sumpin’"),
                toBuffer("Lagunitas IPA"),
                toBuffer("Lagunitas Imperial Stout"),
                toBuffer("Oban 14"),
                toBuffer("Highland Park"),
                toBuffer("Lagavulin"));

        testLog(records);
    }

    @Test
    public void testLargeRecord() throws Exception {
        testLog(toBuffer("dain sundstrom", 4000));
    }

    @Test
    public void testMultipleLargeRecords() throws Exception {
        List<ByteBuffer> records = asList(
                toBuffer("Lagunitas  Little Sumpin’ Sumpin’", 4000),
                toBuffer("Lagunitas IPA", 4000),
                toBuffer("Lagunitas Imperial Stout", 4000),
                toBuffer("Oban 14", 4000),
                toBuffer("Highland Park", 4000),
                toBuffer("Lagavulin", 4000));

        testLog(records);
    }

    @Test
    public void testReadWithoutProperClose() throws Exception {
        testLog(ImmutableList.of(toBuffer("something"), toBuffer("something else")), false);
    }

    private void testLog(ByteBuffer... entries) throws IOException {
        testLog(asList(entries));
    }

    private void testLog(List<ByteBuffer> records) throws IOException {
        testLog(records, true);
    }

    private void testLog(List<ByteBuffer> records, boolean closeWriter) throws IOException {
        for (ByteBuffer entry : records) {
            writer.addRecord(entry, false);
        }

        if (closeWriter) {
            writer.close();
        }

        // test readRecord
        FileChannel fileChannel = new FileInputStream(writer.getFile()).getChannel();
        try {
            LogReader reader = new LogReader(fileChannel, NO_CORRUPTION_MONITOR, true, 0);
            for (ByteBuffer expected : records) {
                expected.flip();
                ByteBuffer actual = reader.readRecord();

                Assert.assertEquals(actual, expected);
            }
            Assert.assertNull(reader.readRecord());
        } finally {
            CloseableUtil.close(fileChannel);
        }
    }

    @Before
    public void setUp() throws Exception {
        writer = Logs.createLogWriter(File.createTempFile("table", ".log"), 42);
    }

    @After
    public void tearDown() throws Exception {
        if (writer != null) {
            writer.delete();
        }
    }

    static ByteBuffer toBuffer(String value) {
        return toBuffer(value, 1);
    }

    static ByteBuffer toBuffer(String value, int times) {
        byte[] bytes = value.getBytes(UTF_8);
        ByteBuffer slice = ByteBuffer.allocate(bytes.length * times);
        slice.put(bytes);
        slice.flip();
        return slice;
    }
}
