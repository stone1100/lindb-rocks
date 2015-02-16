package com.lindb.rocks;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.lindb.rocks.util.CloseableUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/**
 * @author huang_jie
 *         2/9/2015 2:56 PM
 */
public class DBLock {
    private final File lockFile;
    private final FileChannel channel;
    private final FileLock lock;

    public DBLock(File lockFile) throws IOException {
        Preconditions.checkNotNull(lockFile, "lockFile is null");
        this.lockFile = lockFile;

        channel = new RandomAccessFile(lockFile, "rw").getChannel();
        try {
            lock = channel.tryLock();
        } catch (IOException e) {
            CloseableUtil.close(channel);
            throw e;
        }

        if (lock == null) {
            throw new IOException(String.format("Unable to acquire lock on '%s'", lockFile.getAbsoluteFile()));
        }
    }

    public void release() {
        try {
            lock.release();
        } catch (IOException e) {
            Throwables.propagate(e);
        } finally {
            CloseableUtil.close(channel);
        }
    }

    @Override
    public String toString() {
        return "DBLock{" +
                "lockFile=" + lockFile +
                ", lock=" + lock +
                '}';
    }
}
