package com.lindb.rocks.util;

import com.google.common.base.Throwables;
import sun.nio.ch.FileChannelImpl;

import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;

public final class ByteBufferSupport
{
    private static final Method unmap;

    static {
        Method x;
        try {
            x = FileChannelImpl.class.getDeclaredMethod("unmap", MappedByteBuffer.class);
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
        x.setAccessible(true);
        unmap = x;
    }

    private ByteBufferSupport()
    {
    }

    public static void unmap(MappedByteBuffer buffer)
    {
        try {
            unmap.invoke(null, buffer);
        }
        catch (Exception ignored) {
            throw Throwables.propagate(ignored);
        }
    }
}
