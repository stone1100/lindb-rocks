package com.lindb.rocks.util;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class Snappy {
    private Snappy() {
    }

    public interface SPI {
        int uncompress(ByteBuffer compressed, ByteBuffer uncompressed)
                throws IOException;

        int compress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset)
                throws IOException;
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
        public int compress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset)
                throws IOException {
            return org.xerial.snappy.Snappy.compress(input, inputOffset, length, output, outputOffset);
        }
    }

    private static final SPI SNAPPY;

    static {
        SPI attempt = null;
        String[] factories = System.getProperty("rocksdb.snappy", "xerial").split(",");
        for (int i = 0; i < factories.length && attempt == null; i++) {
            String name = factories[i];
            try {
                name = name.trim();
                if ("xerial".equals(name.toLowerCase())) {
                    name = "com.lindb.rocks.util.Snappy$XerialSnappy";
                }
                attempt = (SPI) Thread.currentThread().getContextClassLoader().loadClass(name).newInstance();
            } catch (Throwable ignore) {
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

    public static int compress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset)
            throws IOException {
        return SNAPPY.compress(input, inputOffset, length, output, outputOffset);
    }
}
