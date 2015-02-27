package com.lindb.rocks;

import com.lindb.rocks.table.Snapshot;
import com.lindb.rocks.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static com.google.common.base.Charsets.UTF_8;

public class RacksDBTest {
    File databaseFile = new File("/racksdb/test");
    public static final double STRESS_FACTOR = Double.parseDouble(System.getProperty("STRESS_FACTOR", "1"));

    @Test
    public void testCreateDB() throws IOException {
        Options options = new Options();
        options.createIfMissing(false);
        try {
            new RacksDB(options, databaseFile);
            Assert.assertTrue(false);
        } catch (IllegalArgumentException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void testEmpty() throws Exception {
        Options options = new Options();
        RacksDB db = new RacksDB(options, databaseFile);
        Assert.assertNull(db.get(Bytes.toBytes("foo")));
    }

    @Test
    public void testEmptyBatch() throws Exception {
        // open new db
        Options options = new Options().createIfMissing(true);
        RacksDB db = new RacksDB(options, databaseFile);

        // write an empty batch
        WriteBatch batch = db.createWriteBatch();
        db.write(batch);

        // close the db
        db.close();

        // reopen db
        new RacksDB(options, databaseFile);
    }

    @Test
    public void testReadWrite() throws Exception {
        RacksDB db = new RacksDB(new Options(), databaseFile);
        db.put("foo".getBytes(), "v1".getBytes());
        Assert.assertEquals(new String(db.get("foo".getBytes())), "v1");
        db.put("bar".getBytes(), "v2".getBytes());
        db.put("foo".getBytes(), "v3".getBytes());
        Assert.assertEquals(new String(db.get("foo".getBytes())), "v3");
        Assert.assertEquals(new String(db.get("bar".getBytes())), "v2");
    }

    @Test
    public void testBackgroundCompaction() throws Exception {
        Options options = new Options();
        options.maxOpenFiles(100);
        options.createIfMissing(true);
        RacksDB db = new RacksDB(options, databaseFile);
        Random random = new Random(301);
        for (int i = 0; i < 200000 * STRESS_FACTOR; i++) {
            db.put(randomString(random, 64).getBytes(), new byte[]{0x01}, new WriteOptions().sync(false));
            db.get(randomString(random, 64).getBytes());
            if ((i % 50000) == 0 && i != 0) {
                System.out.println(i + " rows written");
            }
        }
    }

    @Test
    public void testPutDeleteGet() throws Exception {
        RacksDB db = new RacksDB(new Options(), databaseFile);
        db.put(Bytes.toBytes("foo"), Bytes.toBytes("v1"));
        Assert.assertEquals(Bytes.toString(db.get(Bytes.toBytes("foo"))), "v1");
        db.put(Bytes.toBytes("foo"), Bytes.toBytes("v2"));
        Assert.assertEquals(Bytes.toString(db.get(Bytes.toBytes("foo"))), "v2");
        db.delete(Bytes.toBytes("foo"));
        Assert.assertNull(db.get(Bytes.toBytes("foo")));
    }

    @Test
    public void testGetFromImmutableLayer() throws Exception {
        // create db with small write buffer
        RacksDB db = new RacksDB(new Options().writeBufferSize(100000), databaseFile);
        db.put(Bytes.toBytes("foo"), Bytes.toBytes("v1"));
        Assert.assertEquals(Bytes.toString(db.get(Bytes.toBytes("foo"))), "v1");

        // todo Block sync calls

        // Fill memtable
        db.put(Bytes.toBytes("k1"), Bytes.toBytes(longString(100000, 'x')));
        Assert.assertEquals(Bytes.toString(db.get(Bytes.toBytes("foo"))), "v1");
        // Trigger compaction
        db.put(Bytes.toBytes("k2"), Bytes.toBytes(longString(100000, 'y')));
        Assert.assertEquals(Bytes.toString(db.get(Bytes.toBytes("foo"))), "v1");
        // todo Release sync calls
    }

    @Test
    public void testGetFromVersions() throws Exception {
        RacksDB db = new RacksDB(new Options(), databaseFile);
        db.put(Bytes.toBytes("foo"), Bytes.toBytes("v1"));
        db.compactMemTable();
        Assert.assertEquals(Bytes.toString(db.get(Bytes.toBytes("foo"))), "v1");
    }

    @Test
    public void testGetSnapshot() throws Exception {
        RacksDB db = new RacksDB(new Options(), databaseFile);
        // Try with both a short key and a long key
        for (int i = 0; i < 2; i++) {
            String key = (i == 0) ? "foo" : longString(200, 'x');
            db.put(Bytes.toBytes(key), Bytes.toBytes("v1"));
            Snapshot s1 = db.getSnapshot();
            db.put(Bytes.toBytes(key), Bytes.toBytes("v2"));
            Assert.assertEquals(Bytes.toString(db.get(Bytes.toBytes(key))), "v2");
            Assert.assertEquals(get(db, key, s1), "v1");

            db.compactMemTable();
            Assert.assertEquals(Bytes.toString(db.get(Bytes.toBytes(key))), "v2");
            Assert.assertEquals(get(db, key, s1), "v1");
            s1.close();
        }
    }

    @Test
    public void testGetLevel0Ordering() throws Exception {
        RacksDB db = new RacksDB(new Options(), databaseFile);

        // Check that we process level-0 files in correct order.  The code
        // below generates two level-0 files where the earlier one comes
        // before the later one in the level-0 file list since the earlier
        // one has a smaller "smallest" key.
        db.put(Bytes.toBytes("bar"), Bytes.toBytes("b"));
        db.put(Bytes.toBytes("foo"), Bytes.toBytes("v1"));
        db.compactMemTable();
        db.put(Bytes.toBytes("foo"), Bytes.toBytes("v2"));
        db.compactMemTable();
        Assert.assertEquals(Bytes.toString(db.get(Bytes.toBytes("foo"))), "v2");
    }

    @Test
    public void testGetOrderedByLevels() throws Exception {
        RacksDB db = new RacksDB(new Options(), databaseFile);
        db.put(Bytes.toBytes("foo"), Bytes.toBytes("v1"));
        compact(db, "a", "z");
        Assert.assertEquals(Bytes.toString(db.get(Bytes.toBytes("foo"))), "v1");
        db.put(Bytes.toBytes("foo"), Bytes.toBytes("v2"));
        Assert.assertEquals(Bytes.toString(db.get(Bytes.toBytes("foo"))), "v2");
        db.compactMemTable();
        Assert.assertEquals(Bytes.toString(db.get(Bytes.toBytes("foo"))), "v2");
    }

    @Test
    public void testCompactionsOnBigDataSet() throws Exception {
        Options options = new Options();
        options.createIfMissing(true);
        RacksDB db = new RacksDB(options, databaseFile);
        for (int index = 0; index < 5000000; index++) {
            String key = "Key LOOOOOOOOOOOOOOOOOONG KEY " + index;
            String value = "This is element " + index + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABZASDFASDKLFJASDFKJSDFLKSDJFLKJSDHFLKJHSDJFSDFHJASDFLKJSDF";
            db.put(key.getBytes("UTF-8"), value.getBytes("UTF-8"));
        }
    }

    @After
    public void tearDown() throws Exception {
        databaseFile.deleteOnExit();
    }

    private static String randomString(Random random, int length) {
        char[] chars = new char[length];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = (char) ((int) ' ' + random.nextInt(95));
        }
        return new String(chars);
    }

    private static String longString(int length, char character) {
        char[] chars = new char[length];
        Arrays.fill(chars, character);
        return new String(chars);
    }

    private static String get(RacksDB db, String key, Snapshot snapshot) {
        byte[] slice = db.get(Bytes.toBytes(key), new ReadOptions().snapshot(snapshot));
        if (slice == null) {
            return null;
        }
        return Bytes.toString(slice);
    }

    public void compact(RacksDB db, String start, String limit) {
        db.flushMemTable();
        int maxLevelWithFiles = 1;
        for (int level = 2; level < DBConstants.NUM_LEVELS; level++) {
            if (db.numberOfFilesInLevel(level) > 0) {
                maxLevelWithFiles = level;
            }
        }
        for (int level = 0; level < maxLevelWithFiles; level++) {
            db.compactRange(level, Bytes.toBytes(""), Bytes.toBytes("~"));
        }
    }
}