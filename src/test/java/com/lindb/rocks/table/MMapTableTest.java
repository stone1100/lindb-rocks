package com.lindb.rocks.table;


import com.google.common.base.Preconditions;
import com.lindb.rocks.Options;
import com.lindb.rocks.io.BlockEntry;
import com.lindb.rocks.io.BlockHelper;
import com.lindb.rocks.util.Bytes;
import com.lindb.rocks.util.CloseableUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static java.util.Arrays.asList;

public class MMapTableTest {
    private File file;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;

    protected MMapTable createTable(FileChannel fileChannel, Comparator<byte[]> comparator, boolean verifyChecksums)
            throws IOException {
        return new MMapTable(fileChannel, comparator, verifyChecksums);
    }

    @Test
    public void testEmptyFile() throws Exception {
        try {
            createTable(fileChannel, new ByteArrayComparator(), true);
            Assert.assertTrue(false);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testEmptyBlock() throws Exception {
        tableTest(Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    @Test
    public void testSingleEntrySingleBlock() throws Exception {
        tableTest(Integer.MAX_VALUE, Integer.MAX_VALUE,
                BlockHelper.createBlockEntry("name", "dain sundstrom"));
    }

    @Test
    public void testMultipleEntriesWithSingleBlock() throws Exception {
        List<BlockEntry> entries = asList(
                BlockHelper.createBlockEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                BlockHelper.createBlockEntry("beer/ipa", "Lagunitas IPA"),
                BlockHelper.createBlockEntry("beer/stout", "Lagunitas Imperial Stout"),
                BlockHelper.createBlockEntry("scotch/light", "Oban 14"),
                BlockHelper.createBlockEntry("scotch/medium", "Highland Park"),
                BlockHelper.createBlockEntry("scotch/strong", "Lagavulin"));

        for (int i = 1; i < entries.size(); i++) {
            tableTest(Integer.MAX_VALUE, i, entries);
        }
    }

    @Test
    public void testMultipleEntriesWithMultipleBlock() throws Exception {
        List<BlockEntry> entries = asList(
                BlockHelper.createBlockEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                BlockHelper.createBlockEntry("beer/ipa", "Lagunitas IPA"),
                BlockHelper.createBlockEntry("beer/stout", "Lagunitas Imperial Stout"),
                BlockHelper.createBlockEntry("scotch/light", "Oban 14"),
                BlockHelper.createBlockEntry("scotch/medium", "Highland Park"),
                BlockHelper.createBlockEntry("scotch/strong", "Lagavulin")
        );

        // one entry per block
        tableTest(1, Integer.MAX_VALUE, entries);
        System.out.println("*****************************");
        // about 3 blocks
        tableTest(BlockHelper.estimateBlockSize(Integer.MAX_VALUE, entries) / 3, Integer.MAX_VALUE, entries);
    }

    private void tableTest(int blockSize, int blockRestartInterval, BlockEntry... entries) throws IOException {
        tableTest(blockSize, blockRestartInterval, asList(entries));
    }

    private void tableTest(int blockSize, int blockRestartInterval, List<BlockEntry> entries) throws IOException {
        reopenFile();
        Options options = new Options().blockSize(blockSize).blockRestartInterval(blockRestartInterval);
        TableBuilder builder = new TableBuilder(options, fileChannel, new ByteArrayComparator());

        for (BlockEntry entry : entries) {
            builder.add(entry.getKey(), entry.getValue());
        }
        builder.finish();

        MMapTable table = createTable(fileChannel, new ByteArrayComparator(), true);

        SeekingIterator<byte[], byte[]> seekingIterator = table.iterator();
        BlockHelper.assertSequence(seekingIterator, entries);

        seekingIterator.seekToFirst();
        BlockHelper.assertSequence(seekingIterator, entries);

        long lastApproximateOffset = 0;
        for (BlockEntry entry : entries) {
            List<BlockEntry> nextEntries = entries.subList(entries.indexOf(entry), entries.size());
            seekingIterator.seek(entry.getKey());
            BlockHelper.assertSequence(seekingIterator, nextEntries);

            seekingIterator.seek(BlockHelper.before(entry));
            BlockHelper.assertSequence(seekingIterator, nextEntries);

            seekingIterator.seek(BlockHelper.after(entry));
            BlockHelper.assertSequence(seekingIterator, nextEntries.subList(1, nextEntries.size()));

            long approximateOffset = table.getApproximateOffsetOf(entry.getKey());
            Assert.assertTrue(approximateOffset >= lastApproximateOffset);
            lastApproximateOffset = approximateOffset;
        }
        byte[] endKey = new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        seekingIterator.seek(endKey);
        BlockHelper.assertSequence(seekingIterator, Collections.<BlockEntry>emptyList());

        long approximateOffset = table.getApproximateOffsetOf(endKey);
        Assert.assertTrue(approximateOffset >= lastApproximateOffset);

    }

    @Before
    public void setUp() throws Exception {
        reopenFile();
        Preconditions.checkState(0 == fileChannel.position(), "Expected fileChannel.position %s to be 0", fileChannel.position());
    }

    private void reopenFile() throws IOException {
        file = File.createTempFile("table", ".db");
        file.delete();
        randomAccessFile = new RandomAccessFile(file, "rw");
        fileChannel = randomAccessFile.getChannel();
    }

    @After
    public void tearDown() throws Exception {
        CloseableUtil.close(fileChannel);
        CloseableUtil.close(randomAccessFile);
        file.delete();
    }
}