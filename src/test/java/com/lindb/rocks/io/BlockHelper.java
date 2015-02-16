package com.lindb.rocks.io;


import com.lindb.rocks.table.SeekingIterator;
import com.lindb.rocks.util.Bytes;
import org.junit.Assert;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import static com.lindb.rocks.util.Bytes.SIZEOF_BYTE;
import static com.lindb.rocks.util.Bytes.SIZEOF_INT;

public final class BlockHelper {
    private BlockHelper() {
    }

    public static int estimateBlockSize(int blockRestartInterval, List<BlockEntry> entries) {
        if (entries.isEmpty()) {
            return SIZEOF_INT;
        }
        int restartCount = (int) Math.ceil(1.0 * entries.size() / blockRestartInterval);
        return estimateEntriesSize(blockRestartInterval, entries) +
                (restartCount * SIZEOF_INT) +
                SIZEOF_INT;
    }

    @SafeVarargs
    public static <K, V> void assertSequence(SeekingIterator<K, V> seekingIterator, Entry<K, V>... entries) {
        assertSequence(seekingIterator, Arrays.asList(entries));
    }

    public static <K, V> void assertSequence(SeekingIterator<K, V> seekingIterator, Iterable<? extends Entry<K, V>> entries) {
        Assert.assertNotNull(seekingIterator);

        for (Entry<K, V> entry : entries) {
            Assert.assertTrue(seekingIterator.hasNext());
            assertEntryEquals(seekingIterator.peek(), entry);
            assertEntryEquals(seekingIterator.next(), entry);
        }
        Assert.assertFalse(seekingIterator.hasNext());

        try {
            seekingIterator.peek();
            Assert.fail("expected NoSuchElementException");
        } catch (NoSuchElementException expected) {
        }
        try {
            seekingIterator.next();
            Assert.fail("expected NoSuchElementException");
        } catch (NoSuchElementException expected) {
        }
    }

    public static <K, V> void assertEntryEquals(Entry<K, V> actual, Entry<K, V> expected) {
        if (actual.getKey() instanceof byte[]) {
            assertSliceEquals((byte[]) actual.getKey(), (byte[]) expected.getKey());
            assertSliceEquals((byte[]) actual.getValue(), (byte[]) expected.getValue());
        }
        Assert.assertEquals(actual, expected);
    }

    public static void assertSliceEquals(byte[] actual, byte[] expected) {
        Assert.assertEquals(Bytes.toString(actual), Bytes.toString(expected));
    }

    public static String beforeString(Entry<String, ?> expectedEntry) {
        String key = expectedEntry.getKey();
        int lastByte = key.charAt(key.length() - 1);
        return key.substring(0, key.length() - 1) + ((char) (lastByte - 1));
    }

    public static String afterString(Entry<String, ?> expectedEntry) {
        String key = expectedEntry.getKey();
        int lastByte = key.charAt(key.length() - 1);
        return key.substring(0, key.length() - 1) + ((char) (lastByte + 1));
    }

    public static byte[] before(Entry<byte[], ?> expectedEntry) {
        byte[] data = new byte[expectedEntry.getKey().length];
        System.arraycopy(expectedEntry.getKey(), 0, data, 0, data.length);
        data[data.length - 1] = (byte) ((data[data.length - 1] & 0xFF) - 1);
        return data;
    }

    public static byte[] after(Entry<byte[], ?> expectedEntry) {
        byte[] data = new byte[expectedEntry.getKey().length];
        System.arraycopy(expectedEntry.getKey(), 0, data, 0, data.length);
        data[data.length - 1] = (byte) ((data[data.length - 1] & 0xFF) + 1);
        return data;
    }

    public static void main(String[] args) {
        System.out.println((byte) 102);
    }

    public static int estimateEntriesSize(int blockRestartInterval, List<BlockEntry> entries) {
        int size = 0;
        byte[] previousKey = null;
        int restartBlockCount = 0;
        for (BlockEntry entry : entries) {
            int nonSharedBytes;
            if (restartBlockCount < blockRestartInterval) {
                nonSharedBytes = entry.getKey().length - Bytes.calculateSharedBytes(entry.getKey(), previousKey);
            } else {
                nonSharedBytes = entry.getKey().length;
                restartBlockCount = 0;
            }
            size += nonSharedBytes + entry.getValue().length + (SIZEOF_INT * 3); // 3 bytes for sizes

            previousKey = entry.getKey();
            restartBlockCount++;

        }
        return size;
    }

    public static BlockEntry createBlockEntry(String key, String value) {
        return new BlockEntry(Bytes.toBytes(key), Bytes.toBytes(value));
    }
}
