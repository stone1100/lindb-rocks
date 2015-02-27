package com.lindb.rocks.io;

import java.util.Arrays;

/**
 * @author huang_jie
 *         2/25/2015 3:24 PM
 */
public class ByteArrayStream {
    /**
     * The buffer where data is stored.
     */
    protected byte buf[];

    /**
     * The number of valid bytes in the buffer.
     */
    protected int count;

    public ByteArrayStream() {
        this(32);
    }

    public ByteArrayStream(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("Negative initial size: " + size);
        }
        buf = new byte[size];
    }

    public void write(int value) {
        int newCount = count + 4;
        ensureCapacity(newCount);
        buf[count] = (byte) (value >>> 24);
        buf[count + 1] = (byte) (value >>> 16);
        buf[count + 2] = (byte) (value >>> 8);
        buf[count + 3] = (byte) (value);
        count = newCount;
    }

    public void write(byte[] src) {
        write(src, 0, src.length);
    }

    public void write(byte[] src, int off, int len) {
        if ((off < 0) || (off > src.length) || (len < 0) ||
                ((off + len) > src.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        int newCount = count + len;
        ensureCapacity(newCount);
        System.arraycopy(src, off, buf, count, len);
        count = newCount;
    }

    private void ensureCapacity(int len) {
        if (len > buf.length) {
            buf = Arrays.copyOf(buf, Math.max(buf.length << 1, len));
        }
    }

    public void reset() {
        count = 0;
    }

    public byte[] getBuf() {
        return buf;
    }

    public int size() {
        return count;
    }
}
