package com.lindb.rocks;

/**
 * @author huang_jie
 *         2/9/2015 2:43 PM
 */
public class Options {
    public static final int CPU_DATA_MODEL = Integer.getInteger("sun.arch.data.model");

    // We only use MMAP on 64 bit systems since it's really easy to run out of
    // virtual address space on a 32 bit system when all the data is getting mapped
    // into memory.  If you really want to use MMAP anyways, use -Drocksdb.mmap=true
    public static final boolean USE_MMAP = Boolean.parseBoolean(System.getProperty("rocksdb.mmap", "" + (CPU_DATA_MODEL > 32)));
    private boolean createIfMissing = true;
    private boolean errorIfExists;
    private int writeBufferSize = 4 << 20;

    private int maxOpenFiles = 1000;

    private int blockRestartInterval = 16;
    private int blockSize = 4 * 1024;
    private CompressionType compressionType = CompressionType.SNAPPY;
    private boolean verifyChecksums = true;
    private boolean paranoidChecks;
    private long cacheSize;


    static void checkArgNotNull(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException("The " + name + " argument cannot be null");
        }
    }

    public boolean createIfMissing() {
        return createIfMissing;
    }

    public Options createIfMissing(boolean createIfMissing) {
        this.createIfMissing = createIfMissing;
        return this;
    }

    public boolean errorIfExists() {
        return errorIfExists;
    }

    public Options errorIfExists(boolean errorIfExists) {
        this.errorIfExists = errorIfExists;
        return this;
    }

    public int writeBufferSize() {
        return writeBufferSize;
    }

    public Options writeBufferSize(int writeBufferSize) {
        this.writeBufferSize = writeBufferSize;
        return this;
    }

    public int maxOpenFiles() {
        return maxOpenFiles;
    }

    public Options maxOpenFiles(int maxOpenFiles) {
        this.maxOpenFiles = maxOpenFiles;
        return this;
    }

    public int blockRestartInterval() {
        return blockRestartInterval;
    }

    public Options blockRestartInterval(int blockRestartInterval) {
        this.blockRestartInterval = blockRestartInterval;
        return this;
    }

    public int blockSize() {
        return blockSize;
    }

    public Options blockSize(int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    public CompressionType compressionType() {
        return compressionType;
    }

    public Options compressionType(CompressionType compressionType) {
        checkArgNotNull(compressionType, "compressionType");
        this.compressionType = compressionType;
        return this;
    }

    public boolean verifyChecksums() {
        return verifyChecksums;
    }

    public Options verifyChecksums(boolean verifyChecksums) {
        this.verifyChecksums = verifyChecksums;
        return this;
    }

    public long cacheSize() {
        return cacheSize;
    }

    public Options cacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
        return this;
    }

    public boolean paranoidChecks() {
        return paranoidChecks;
    }

    public Options paranoidChecks(boolean paranoidChecks) {
        this.paranoidChecks = paranoidChecks;
        return this;
    }
}
