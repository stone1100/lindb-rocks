package com.lindb.rocks.table;


import com.lindb.rocks.DBComparator;

public class CustomUserComparator implements UserComparator {
    private final DBComparator comparator;

    public CustomUserComparator(DBComparator comparator) {
        this.comparator = comparator;
    }

    @Override
    public String name() {
        return comparator.name();
    }

    @Override
    public byte[] findShortestSeparator(byte[] start, byte[] end) {
        return comparator.findShortestSeparator(start, end);
    }

    @Override
    public byte[] findShortSuccessor(byte[] key) {
        return comparator.findShortSuccessor(key);
    }

    @Override
    public int compare(byte[] left, byte[] right) {
        return comparator.compare(left, right);
    }
}
