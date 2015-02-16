package com.lindb.rocks;

public class WriteOptions {
    private boolean sync;
    private boolean snapshot;

    private static class WriteOptionsHolder {
        public static WriteOptions instance = new WriteOptions();
    }

    public static WriteOptions defaultWriteOptions() {
        return WriteOptionsHolder.instance;
    }

    public boolean sync() {
        return sync;
    }

    public WriteOptions sync(boolean sync) {
        this.sync = sync;
        return this;
    }

    public boolean snapshot() {
        return snapshot;
    }

    public WriteOptions snapshot(boolean snapshot) {
        this.snapshot = snapshot;
        return this;
    }
}
