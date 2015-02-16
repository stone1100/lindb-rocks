package com.lindb.rocks.util;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author huang_jie
 *         2/9/2015 3:00 PM
 */
public class CloseableUtil {

    private CloseableUtil() {
    }

    public static void close(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException ignored) {
        }
    }

    public static void close(Closeable closeable, boolean isThrowException) throws IOException {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException e) {
            throw e;
        }
    }
}
