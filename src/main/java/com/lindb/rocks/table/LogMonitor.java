package com.lindb.rocks.table;

public interface LogMonitor {
    void corruption(long bytes, String reason);

    void corruption(long bytes, Throwable reason);
}
