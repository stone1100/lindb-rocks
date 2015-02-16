package com.lindb.rocks.util;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class Snappy {
    private Snappy() {
    }

    public interface SPI {
        int uncompress(ByteBuffer compressed, ByteBuffer uncompressed)
                throws IOException;

        int uncompress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset)
                throws IOException;

        int compress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset)
                throws IOException;

        byte[] compress(String text)
                throws IOException;

        int maxCompressedLength(int length);
    }

    public static class XerialSnappy
            implements SPI {
        static {
            // Make sure that the JNI libs are fully loaded.
            try {
                org.xerial.snappy.Snappy.compress("test");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int uncompress(ByteBuffer compressed, ByteBuffer uncompressed)
                throws IOException {
            return org.xerial.snappy.Snappy.uncompress(compressed, uncompressed);
        }

        @Override
        public int uncompress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset)
                throws IOException {
            return org.xerial.snappy.Snappy.uncompress(input, inputOffset, length, output, outputOffset);
        }

        @Override
        public int compress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset)
                throws IOException {
            return org.xerial.snappy.Snappy.compress(input, inputOffset, length, output, outputOffset);
        }

        @Override
        public byte[] compress(String text)
                throws IOException {
            return org.xerial.snappy.Snappy.compress(text);
        }

        @Override
        public int maxCompressedLength(int length) {
            return org.xerial.snappy.Snappy.maxCompressedLength(length);
        }
    }

    private static final SPI SNAPPY;

    static {
        SPI attempt = null;
        String[] factories = System.getProperty("rocksdb.snappy", "iq80,xerial").split(",");
        for (int i = 0; i < factories.length && attempt == null; i++) {
            String name = factories[i];
            try {
                name = name.trim();
                if ("xerial".equals(name.toLowerCase())) {
                    name = "com.lindb.rocks.util.Snappy$XerialSnappy";
                }
                attempt = (SPI) Thread.currentThread().getContextClassLoader().loadClass(name).newInstance();
            } catch (Throwable e) {
            }
        }
        SNAPPY = attempt;
    }

    public static boolean available() {
        return SNAPPY != null;
    }

    public static void uncompress(ByteBuffer compressed, ByteBuffer uncompressed)
            throws IOException {
        SNAPPY.uncompress(compressed, uncompressed);
    }

    public static void uncompress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset)
            throws IOException {
        SNAPPY.uncompress(input, inputOffset, length, output, outputOffset);
    }

    public static int compress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset)
            throws IOException {
        return SNAPPY.compress(input, inputOffset, length, output, outputOffset);
    }

    public static byte[] compress(String text)
            throws IOException {
        return SNAPPY.compress(text);
    }

    public static int maxCompressedLength(int length) {
        return SNAPPY.maxCompressedLength(length);
    }
}
