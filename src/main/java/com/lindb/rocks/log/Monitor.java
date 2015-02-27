package com.lindb.rocks.log;

public interface Monitor {
    void corruption(long bytes, String reason);

    void corruption(long bytes, Throwable reason);
}
