package com.lindb.rocks.util;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class IntVector {
    private int size;
    private int[] values;

    public IntVector(int initialCapacity) {
        this.values = new int[initialCapacity];
    }

    public int size() {
        return size;
    }

    public void clear() {
        size = 0;
    }

    public void add(int value) {
        ensureCapacity(size + 1);

        values[size++] = value;
    }

    private void ensureCapacity(int minCapacity) {
        if (values.length >= minCapacity) {
            return;
        }

        int newLength = values.length;
        if (newLength == 0) {
            newLength = 1;
        } else {
            newLength <<= 1;

        }
        values = Arrays.copyOf(values, newLength);
    }

    public void write(ByteBuffer out) {
        for (int index = 0; index < size; index++) {
            out.putInt(values[index]);
        }
    }
}
