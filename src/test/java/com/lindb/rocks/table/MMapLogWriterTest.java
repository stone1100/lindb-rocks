package com.lindb.rocks.table;

import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MMapLogWriterTest {
    @Test
    public void testLogRecordBounds()
            throws Exception {
        File file = File.createTempFile("test", ".log");
        try {
            int recordSize = LogConstants.BLOCK_SIZE - LogConstants.CHUNK_HEADER_SIZE;
            ByteBuffer record = ByteBuffer.allocate(recordSize);

            MMapLogWriter writer = new MMapLogWriter(file, 10);
            writer.addRecord(record, false);
            writer.close();

            LogMonitor logMonitor = new AssertNoCorruptionLogMonitor();

            FileChannel channel = new FileInputStream(file).getChannel();

            LogReader logReader = new LogReader(channel, logMonitor, true, 0);

            int count = 0;
            for (ByteBuffer slice = logReader.readRecord(); slice != null; slice = logReader.readRecord()) {
                assertEquals(slice.remaining(), recordSize);
                count++;
            }
            assertEquals(count, 1);
        } finally {
            file.delete();
        }
    }

    private static class AssertNoCorruptionLogMonitor
            implements LogMonitor {
        @Override
        public void corruption(long bytes, String reason) {
            fail("corruption at " + bytes + " reason: " + reason);
        }

        @Override
        public void corruption(long bytes, Throwable reason) {
            fail("corruption at " + bytes + " reason: " + reason);
        }
    }
}