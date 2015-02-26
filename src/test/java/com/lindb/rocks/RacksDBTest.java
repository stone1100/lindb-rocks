package com.lindb.rocks;

import com.lindb.rocks.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;

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

}