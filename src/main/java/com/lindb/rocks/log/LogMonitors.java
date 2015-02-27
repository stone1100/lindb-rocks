package com.lindb.rocks.log;

import com.lindb.rocks.log.Monitor;

public final class LogMonitors {
    public static Monitor throwExceptionMonitor() {
        return new Monitor() {
            @Override
            public void corruption(long bytes, String reason) {
                throw new RuntimeException(String.format("corruption of %s bytes: %s", bytes, reason));
            }

            @Override
            public void corruption(long bytes, Throwable reason) {
                throw new RuntimeException(String.format("corruption of %s bytes", bytes), reason);
            }
        };
    }

    public static Monitor logMonitor() {
        return new Monitor() {
            @Override
            public void corruption(long bytes, String reason) {
                System.out.println(String.format("corruption of %s bytes: %s", bytes, reason));
            }

            @Override
            public void corruption(long bytes, Throwable reason) {
                System.out.println(String.format("corruption of %s bytes", bytes));
                reason.printStackTrace();
            }
        };
    }

    private LogMonitors() {
    }
}
