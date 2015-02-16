package com.lindb.rocks.table;


import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

public class Snapshot implements Closeable {
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Version version;
    private final long lastSequence;

    public Snapshot(Version version, long lastSequence) {
        this.version = version;
        this.lastSequence = lastSequence;
        this.version.retain();
    }

    @Override
    public void close() {
        // This is an end user API.. he might screw up and close multiple times.
        // but we don't want the version reference count going bad.
        if (closed.compareAndSet(false, true)) {
            this.version.release();
        }
    }

    public long getLastSequence() {
        return lastSequence;
    }

    public Version getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return Long.toString(lastSequence);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Snapshot snapshot = (Snapshot) o;

        if (lastSequence != snapshot.lastSequence) {
            return false;
        }
        if (!version.equals(snapshot.version)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = version.hashCode();
        result = 31 * result + (int) (lastSequence ^ (lastSequence >>> 32));
        return result;
    }
}
