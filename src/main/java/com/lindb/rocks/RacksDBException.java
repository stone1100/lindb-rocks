package com.lindb.rocks;

/**
 * @author huang_jie
 *         2/10/2015 3:40 PM
 */
public class RacksDBException extends RuntimeException {
    public RacksDBException() {
    }

    public RacksDBException(String s) {
        super(s);
    }

    public RacksDBException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public RacksDBException(Throwable throwable) {
        super(throwable);
    }
}
