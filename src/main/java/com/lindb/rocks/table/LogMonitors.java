package com.lindb.rocks.table;

import com.lindb.rocks.log.LogMonitor;

public final class LogMonitors {
    public static LogMonitor throwExceptionMonitor() {
        return new LogMonitor() {
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

    public static LogMonitor logMonitor() {
        return new LogMonitor() {
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
