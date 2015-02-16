package com.lindb.rocks.table;

import com.lindb.rocks.LookupKey;
import com.lindb.rocks.LookupResult;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MemTableTest {

    @Test
    public void testIsEmpty() throws Exception {
        MemTable table = new MemTable(new InternalKeyComparator(new ByteArrayComparator()));
        assertTrue(table.isEmpty());
    }

    @Test
    public void testAddAndGet() throws Exception {
        MemTable table = new MemTable(new InternalKeyComparator(new ByteArrayComparator()));
        table.add("key".getBytes(), "value".getBytes(), 1, ValueType.VALUE);

        LookupResult result = table.get(new LookupKey("key".getBytes(), 1));
        assertEquals("value", new String(result.getValue()));
    }

}