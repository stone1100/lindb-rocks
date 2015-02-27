package com.lindb.rocks;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lindb.rocks.log.Log;
import com.lindb.rocks.log.Log.Reader;
import com.lindb.rocks.log.Log.Writer;
import com.lindb.rocks.log.LogMonitors;
import com.lindb.rocks.log.Monitor;
import com.lindb.rocks.table.*;
import com.lindb.rocks.util.Bytes;
import com.lindb.rocks.util.Snappy;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.collect.Lists.newArrayList;
import static com.lindb.rocks.DBConstants.*;
import static com.lindb.rocks.table.SequenceNumber.MAX_SEQUENCE_NUMBER;
import static com.lindb.rocks.table.ValueType.*;

/**
 * @author huang_jie
 *         2/9/2015 2:26 PM
 */
public class RacksDB implements Iterable<Map.Entry<byte[], byte[]>>, Closeable {
    private final Options options;
    private final File databasePath;
    private final TableCache tableCache;
    private final DBLock dbLock;
    private final VersionSet versions;
    private final InternalKeyComparator internalKeyComparator;
    private final ExecutorService compactionExecutor;

    private final Object suspensionMutex = new Object();
    private final AtomicBoolean shuttingDown = new AtomicBoolean();
    private final ReentrantLock mutex = new ReentrantLock();
    private final Condition backgroundCondition = mutex.newCondition();
    private final List<Long> pendingOutputs = Lists.newArrayList();
    private Writer log;
    private MemTable memTable;
    private MemTable immutableMemTable;
    private Future backgroundCompaction;
    private ManualCompaction manualCompaction;
    private int suspensionCounter;

    private volatile Throwable backgroundException;

    public RacksDB(Options options, File databasePath) throws IOException {
        Preconditions.checkNotNull(options, "options is null");
        Preconditions.checkNotNull(databasePath, "databasePath is null");

        if (!databasePath.exists()) {
            databasePath.mkdirs();
        }
        Preconditions.checkArgument(databasePath.exists(), "Database directory '%s' does not exist and could not be created", databasePath);
        Preconditions.checkArgument(databasePath.isDirectory(), "Database directory '%s' is not a directory", databasePath);
        this.databasePath = databasePath;

        this.options = options;
        if (this.options.compressionType() == CompressionType.SNAPPY && !Snappy.available()) {
            //Disable snappy if it's not available
            this.options.compressionType(CompressionType.NONE);
        }

        internalKeyComparator = new InternalKeyComparator(new ByteArrayComparator());
        memTable = new MemTable(internalKeyComparator);


        ThreadFactory compactionThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("rocksdb-compaction-%s")
                .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        System.out.printf("%s%n", t);
                        e.printStackTrace();
                    }
                })
                .build();
        compactionExecutor = Executors.newSingleThreadExecutor(compactionThreadFactory);

        // Reserve ten files or so for other uses and give the rest to TableCache.
        int tableCacheSize = options.maxOpenFiles() - 10;
        tableCache = new TableCache(databasePath, tableCacheSize, new InternalUserComparator(internalKeyComparator), options.verifyChecksums());


        mutex.lock();
        try {
            // lock the database dir
            dbLock = new DBLock(new File(databasePath, FileName.lockFileName()));
            // verify the "current" file
            File currentFile = new File(databasePath, FileName.currentFileName());
            if (!currentFile.canRead()) {
                Preconditions.checkArgument(options.createIfMissing(), "Database '%s' does not exist and the create if missing option is disabled", databasePath);
            } else {
                Preconditions.checkArgument(!options.errorIfExists(), "Database '%s' exists and the error if exists option is enabled", databasePath);
            }

            versions = new VersionSet(databasePath, tableCache, internalKeyComparator);
            // load  (and recover) current version
            versions.recover();
            // Recover from all newer log files than the ones named in the
            // descriptor (new log files may have been added by the previous
            // incarnation without registering them in the descriptor).
            //
            // Note that PrevLogNumber() is no longer used, but we pay
            // attention to it in case we are recovering a database
            // produced by an older version of leveldb.
            long minLogNumber = versions.getLogNumber();
            long previousLogNumber = versions.getPrevLogNumber();
            List<File> fileNames = FileName.listFiles(databasePath);
            List<Long> logs = Lists.newArrayList();
            for (File filename : fileNames) {
                FileName.FileInfo fileInfo = FileName.parseFileName(filename);
                if (fileInfo != null && fileInfo.getFileType() == FileName.FileType.LOG &&
                        ((fileInfo.getFileNumber() >= minLogNumber) || (fileInfo.getFileNumber() == previousLogNumber))) {
                    logs.add(fileInfo.getFileNumber());
                }
            }

            // Recover in the order in which the logs were generated
            VersionEdit edit = new VersionEdit();
            Collections.sort(logs);
            for (Long fileNumber : logs) {
                long maxSequence = recoverLogFile(fileNumber, edit);
                if (versions.getLastSequence() < maxSequence) {
                    versions.setLastSequence(maxSequence);
                }
            }

            // open transaction log
            long logFileNumber = versions.getNextFileNumber();
            this.log = Log.createWriter(new File(databasePath, FileName.logFileName(logFileNumber)), logFileNumber);
            edit.setLogNumber(log.getFileNumber());

            // apply recovered edits
            versions.logAndApply(edit);

            // cleanup unused files
            deleteObsoleteFiles();

            // schedule compactions
            maybeScheduleCompaction();
        } finally {
            mutex.unlock();
        }
    }

    @Override
    public void close() {
        if (shuttingDown.getAndSet(true)) {
            return;
        }
        mutex.lock();
        try {
            while (backgroundCompaction != null) {
                backgroundCondition.awaitUninterruptibly();
            }
        } finally {
            mutex.unlock();
        }

        compactionExecutor.shutdown();
        try {
            compactionExecutor.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            versions.destroy();
        } catch (IOException ignored) {
        }
        try {
            log.close();
        } catch (IOException ignored) {
        }
        tableCache.close();
        dbLock.release();
    }

    public String getProperty(String name) {
        checkBackgroundException();
        return null;
    }

    private void deleteObsoleteFiles() {
        Preconditions.checkState(mutex.isHeldByCurrentThread());

        // Make a set of all of the live files
        List<Long> live = newArrayList(this.pendingOutputs);
        for (FileMetaData fileMetaData : versions.getLiveFiles()) {
            live.add(fileMetaData.getNumber());
        }
        for (File file : FileName.listFiles(databasePath)) {
            FileName.FileInfo fileInfo = FileName.parseFileName(file);
            if (fileInfo == null) {
                continue;
            }
            long number = fileInfo.getFileNumber();
            boolean keep = true;
            switch (fileInfo.getFileType()) {
                case LOG:
                    keep = ((number >= versions.getLogNumber()) ||
                            (number == versions.getPrevLogNumber()));
                    break;
                case DESCRIPTOR:
                    // Keep my manifest file, and any newer incarnations'
                    // (in case there is a race that allows other incarnations)
                    keep = (number >= versions.getManifestFileNumber());
                    break;
                case TABLE:
                    keep = live.contains(number);
                    break;
                case TEMP:
                    // Any temp files that are currently being written to must
                    // be recorded in pending_outputs_, which is inserted into "live"
                    keep = live.contains(number);
                    break;
                case CURRENT:
                case DB_LOCK:
                case INFO_LOG:
                    keep = true;
                    break;
            }

            if (!keep) {
                if (fileInfo.getFileType() == FileName.FileType.TABLE) {
                    tableCache.evict(number);
                }

                file.delete();
            }
        }
    }

    public void flushMemTable() {
        mutex.lock();
        try {
            // force compaction
            makeRoomForWrite(true);

            // todo bg_error code
            while (immutableMemTable != null) {
                backgroundCondition.awaitUninterruptibly();
            }

        } finally {
            mutex.unlock();
        }
    }

    public void compactRange(int level, byte[] start, byte[] end) {
        Preconditions.checkArgument(level >= 0, "level is negative");
        Preconditions.checkArgument(level + 1 < NUM_LEVELS, "level is greater than or equal to %s", NUM_LEVELS);
        Preconditions.checkNotNull(start, "start is null");
        Preconditions.checkNotNull(end, "end is null");

        mutex.lock();
        try {
            while (this.manualCompaction != null) {
                backgroundCondition.awaitUninterruptibly();
            }
            ManualCompaction manualCompaction = new ManualCompaction(level, start, end);
            this.manualCompaction = manualCompaction;

            maybeScheduleCompaction();

            while (this.manualCompaction == manualCompaction) {
                backgroundCondition.awaitUninterruptibly();
            }
        } finally {
            mutex.unlock();
        }

    }

    private void maybeScheduleCompaction() {
        Preconditions.checkState(mutex.isHeldByCurrentThread());

        if (backgroundCompaction != null) {
            // Already scheduled
        } else if (shuttingDown.get()) {
            // DB is being shutdown; no more background compactions
        } else if (immutableMemTable == null && manualCompaction == null && !versions.needsCompaction()) {
            // No work to be done
        } else {
            backgroundCompaction = compactionExecutor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    try {
                        backgroundCall();
                    } catch (DatabaseShutdownException ignored) {
                    } catch (Throwable e) {
                        backgroundException = e;
                    }
                    return null;
                }
            });
        }
    }


    public void suspendCompactions()
            throws InterruptedException {
        compactionExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    synchronized (suspensionMutex) {
                        suspensionCounter++;
                        suspensionMutex.notifyAll();
                        while (suspensionCounter > 0 && !compactionExecutor.isShutdown()) {
                            suspensionMutex.wait(500);
                        }
                    }
                } catch (InterruptedException e) {
                }
            }
        });
        synchronized (suspensionMutex) {
            while (suspensionCounter < 1) {
                suspensionMutex.wait();
            }
        }
    }

    public void resumeCompactions() {
        synchronized (suspensionMutex) {
            suspensionCounter--;
            suspensionMutex.notifyAll();
        }
    }

    public void compactRange(byte[] begin, byte[] end)
            throws RacksDBException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public void checkBackgroundException() {
        Throwable e = backgroundException;
        if (e != null) {
            throw new BackgroundProcessingException(e);
        }
    }

    private void backgroundCall()
            throws IOException {
        mutex.lock();
        try {
            if (backgroundCompaction == null) {
                return;
            }

            try {
                if (!shuttingDown.get()) {
                    backgroundCompaction();
                }
            } finally {
                backgroundCompaction = null;
            }
        } finally {
            try {
                // Previous compaction may have produced too many files in a level,
                // so reschedule another compaction if needed.
                maybeScheduleCompaction();
            } finally {
                try {
                    backgroundCondition.signalAll();
                } finally {
                    mutex.unlock();
                }
            }
        }
    }

    private void backgroundCompaction()
            throws IOException {
        Preconditions.checkState(mutex.isHeldByCurrentThread());

        compactMemTableInternal();

        Compaction compaction;
        if (manualCompaction != null) {
            compaction = versions.compactRange(manualCompaction.level,
                    new InternalKey(manualCompaction.start, MAX_SEQUENCE_NUMBER, VALUE),
                    new InternalKey(manualCompaction.end, 0, DELETION));
        } else {
            compaction = versions.pickCompaction();
        }

        if (compaction == null) {
            // no compaction
        } else if (manualCompaction == null && compaction.isTrivialMove()) {
            // Move file to next level
            Preconditions.checkState(compaction.getLevelInputs().size() == 1);
            FileMetaData fileMetaData = compaction.getLevelInputs().get(0);
            compaction.getEdit().deleteFile(compaction.getLevel(), fileMetaData.getNumber());
            compaction.getEdit().addFile(compaction.getLevel() + 1, fileMetaData);
            versions.logAndApply(compaction.getEdit());
            // log
        } else {
            CompactionState compactionState = new CompactionState(compaction);
            doCompactionWork(compactionState);
            cleanupCompaction(compactionState);
        }

        // manual compaction complete
        if (manualCompaction != null) {
            manualCompaction = null;
        }
    }


    private void cleanupCompaction(CompactionState compactionState) {
        Preconditions.checkState(mutex.isHeldByCurrentThread());

        if (compactionState.builder != null) {
            compactionState.builder.abandon();
        } else {
            Preconditions.checkArgument(compactionState.outfile == null);
        }

        for (FileMetaData output : compactionState.outputs) {
            pendingOutputs.remove(output.getNumber());
        }
    }

    private long recoverLogFile(long fileNumber, VersionEdit edit)
            throws IOException {
        Preconditions.checkState(mutex.isHeldByCurrentThread());
        File file = new File(databasePath, FileName.logFileName(fileNumber));
        try (FileChannel channel = new FileInputStream(file).getChannel()) {
            Monitor logMonitor = LogMonitors.logMonitor();
            Reader logReader = new Reader(channel, logMonitor, true, 0);

            // Log(options_.info_log, "Recovering log #%llu", (unsigned long long) log_number);

            // Read all the records and add to a memtable
            long maxSequence = 0;
            MemTable memTable = null;
            for (ByteBuffer record = logReader.readRecord(); record != null; record = logReader.readRecord()) {
                // read header
                if (record.remaining() < RECORD_HEADER_SIZE) {
                    logMonitor.corruption(record.remaining(), "log record too small");
                    continue;
                }
                long sequenceBegin = record.getLong();
                int updateSize = record.getInt();

                // read entries
                WriteBatch writeBatch = readWriteBatch(record, updateSize);

                // apply entries to memTable
                if (memTable == null) {
                    memTable = new MemTable(internalKeyComparator);
                }
                writeBatch.forEach(new InsertIntoHandler(memTable, sequenceBegin));

                // update the maxSequence
                long lastSequence = sequenceBegin + updateSize - 1;
                if (lastSequence > maxSequence) {
                    maxSequence = lastSequence;
                }

                // flush mem table if necessary
                if (memTable.approximateMemoryUsage() > options.writeBufferSize()) {
                    writeLevel0Table(memTable, edit, null);
                    memTable = null;
                }
            }

            // flush mem table
            if (memTable != null && !memTable.isEmpty()) {
                writeLevel0Table(memTable, edit, null);
            }

            return maxSequence;
        }
    }

    public byte[] get(byte[] key)
            throws RacksDBException {
        return get(key, new ReadOptions());
    }

    public byte[] get(byte[] key, ReadOptions options)
            throws RacksDBException {
        checkBackgroundException();
        LookupKey lookupKey;
        mutex.lock();
        try {
            Snapshot snapshot = getSnapshot(options);
            lookupKey = new LookupKey(key, snapshot.getLastSequence());

            // First look in the memory table, then in the immutable memory table (if any).
            LookupResult lookupResult = memTable.get(lookupKey);
            if (lookupResult != null) {
                byte[] value = lookupResult.getValue();
                if (value == null) {
                    return null;
                }
                return value;
            }
            if (immutableMemTable != null) {
                lookupResult = immutableMemTable.get(lookupKey);
                if (lookupResult != null) {
                    byte[] value = lookupResult.getValue();
                    if (value == null) {
                        return null;
                    }
                    return value;
                }
            }
        } finally {
            mutex.unlock();
        }

        // Not in memTables; try live files in level order
        LookupResult lookupResult = versions.get(lookupKey);

        // schedule compaction if necessary
        mutex.lock();
        try {
            if (versions.needsCompaction()) {
                maybeScheduleCompaction();
            }
        } finally {
            mutex.unlock();
        }

        if (lookupResult != null) {
            byte[] value = lookupResult.getValue();
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    public void put(byte[] key, byte[] value) throws RacksDBException {
        put(key, value, WriteOptions.defaultWriteOptions());
    }

    public Snapshot put(byte[] key, byte[] value, WriteOptions options) throws RacksDBException {
        return writeInternal(new WriteBatch().put(key, value), options);
    }

    public void delete(byte[] key) throws RacksDBException {
        writeInternal(new WriteBatch().delete(key), new WriteOptions());
    }

    public Snapshot delete(byte[] key, WriteOptions options) throws RacksDBException {
        return writeInternal(new WriteBatch().delete(key), options);
    }

    public void write(WriteBatch updates) throws RacksDBException {
        writeInternal(updates, new WriteOptions());
    }

    public Snapshot write(WriteBatch updates, WriteOptions options) throws RacksDBException {
        return writeInternal(updates, options);
    }

    public Snapshot writeInternal(WriteBatch updates, WriteOptions options) throws RacksDBException {
        checkBackgroundException();
        mutex.lock();
        try {
            long sequenceEnd;
            if (updates.size() != 0) {
                makeRoomForWrite(false);

                // Get sequence numbers for this change set
                long sequenceBegin = versions.getLastSequence() + 1;
                sequenceEnd = sequenceBegin + updates.size() - 1;

                // Reserve this sequence in the version set
                versions.setLastSequence(sequenceEnd);

                // Log write
                ByteBuffer record = writeWriteBatch(updates, sequenceBegin);
                try {
                    log.addRecord(record, options.sync());
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }

                // Update mem table
                updates.forEach(new InsertIntoHandler(memTable, sequenceBegin));
            } else {
                sequenceEnd = versions.getLastSequence();
            }

            if (options.snapshot()) {
                return new Snapshot(versions.getCurrent(), sequenceEnd);
            } else {
                return null;
            }
        } finally {
            mutex.unlock();
        }
    }

    public WriteBatch createWriteBatch() {
        checkBackgroundException();
        return new WriteBatch();
    }

    @Override
    public SeekingIteratorAdapter iterator() {
        return iterator(new ReadOptions());
    }

    public SeekingIteratorAdapter iterator(ReadOptions options) {
        checkBackgroundException();
        mutex.lock();
        try {
            DBIterator rawIterator = internalIterator();

            // filter any entries not visible in our snapshot
            Snapshot snapshot = getSnapshot(options);
            SnapshotSeekingIterator snapshotIterator = new SnapshotSeekingIterator(rawIterator, snapshot, internalKeyComparator.getUserComparator());
            return new SeekingIteratorAdapter(snapshotIterator);
        } finally {
            mutex.unlock();
        }
    }

    SeekingIterable<InternalKey, byte[]> internalIterable() {
        return new SeekingIterable<InternalKey, byte[]>() {
            @Override
            public DBIterator iterator() {
                return internalIterator();
            }
        };
    }

    DBIterator internalIterator() {
        mutex.lock();
        try {
            // merge together the memTable, immutableMemTable, and tables in version set
            MemTable.MemTableIterator iterator = null;
            if (immutableMemTable != null) {
                iterator = immutableMemTable.iterator();
            }
            Version current = versions.getCurrent();
            return new DBIterator(memTable.iterator(), iterator, current.getLevel0Files(), current.getLevelIterators(), internalKeyComparator);
        } finally {
            mutex.unlock();
        }
    }

    public Snapshot getSnapshot() {
        checkBackgroundException();
        mutex.lock();
        try {
            return new Snapshot(versions.getCurrent(), versions.getLastSequence());
        } finally {
            mutex.unlock();
        }
    }

    private Snapshot getSnapshot(ReadOptions options) {
        Snapshot snapshot;
        if (options.snapshot() != null) {
            snapshot = options.snapshot();
        } else {
            snapshot = new Snapshot(versions.getCurrent(), versions.getLastSequence());
            snapshot.close(); // To avoid holding the snapshot active..
        }
        return snapshot;
    }

    private void makeRoomForWrite(boolean force) {
        Preconditions.checkState(mutex.isHeldByCurrentThread());

        boolean allowDelay = !force;

        while (true) {
            // todo background processing system need work
//            if (!bg_error_.ok()) {
//              // Yield previous error
//              s = bg_error_;
//              break;
//            } else
            if (allowDelay && versions.numberOfFilesInLevel(0) > L0_SLOWDOWN_WRITES_TRIGGER) {
                // We are getting close to hitting a hard limit on the number of
                // L0 files.  Rather than delaying a single write by several
                // seconds when we hit the hard limit, start delaying each
                // individual write by 1ms to reduce latency variance.  Also,
                // this delay hands over some CPU to the compaction thread in
                // case it is sharing the same core as the writer.
                try {
                    mutex.unlock();
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } finally {
                    mutex.lock();
                }

                // Do not delay a single write more than once
                allowDelay = false;
            } else if (!force && memTable.approximateMemoryUsage() <= options.writeBufferSize()) {
                // There is room in current memory table
                break;
            } else if (immutableMemTable != null) {
                // We have filled up the current memory table, but the previous
                // one is still being compacted, so we wait.
                backgroundCondition.awaitUninterruptibly();
            } else if (versions.numberOfFilesInLevel(0) >= L0_STOP_WRITES_TRIGGER) {
                // There are too many level-0 files.
//                Log(options_.info_log, "waiting...\n");
                backgroundCondition.awaitUninterruptibly();
            } else {
                // Attempt to switch to a new memory table and trigger compaction of old
                Preconditions.checkState(versions.getPrevLogNumber() == 0);

                // close the existing log
                try {
                    log.close();
                } catch (IOException e) {
                    throw new RuntimeException("Unable to close log file " + log.getFile(), e);
                }

                // open a new log
                long logNumber = versions.getNextFileNumber();
                try {
                    this.log = Log.createWriter(new File(databasePath, FileName.logFileName(logNumber)), logNumber);
                } catch (IOException e) {
                    throw new RuntimeException("Unable to open new log file " +
                            new File(databasePath, FileName.logFileName(logNumber)).getAbsoluteFile(), e);
                }

                // create a new mem table
                immutableMemTable = memTable;
                memTable = new MemTable(internalKeyComparator);

                // Do not force another compaction there is space available
                force = false;

                maybeScheduleCompaction();
            }
        }
    }

    public void compactMemTable()
            throws IOException {
        mutex.lock();
        try {
            compactMemTableInternal();
        } finally {
            mutex.unlock();
        }
    }

    private void compactMemTableInternal()
            throws IOException {
        Preconditions.checkState(mutex.isHeldByCurrentThread());
        if (immutableMemTable == null) {
            return;
        }

        try {
            // Save the contents of the memtable as a new Table
            VersionEdit edit = new VersionEdit();
            Version base = versions.getCurrent();
            writeLevel0Table(immutableMemTable, edit, base);

            if (shuttingDown.get()) {
                throw new DatabaseShutdownException("Database shutdown during memtable compaction");
            }

            // Replace immutable memtable with the generated Table
            edit.setPreviousLogNumber(0);
            edit.setLogNumber(log.getFileNumber());  // Earlier logs no longer needed
            versions.logAndApply(edit);

            immutableMemTable = null;

            deleteObsoleteFiles();
        } finally {
            backgroundCondition.signalAll();
        }
    }

    private void writeLevel0Table(MemTable mem, VersionEdit edit, Version base)
            throws IOException {
        Preconditions.checkState(mutex.isHeldByCurrentThread());

        // skip empty mem table
        if (mem.isEmpty()) {
            return;
        }

        // write the memory table to a new sstable
        long fileNumber = versions.getNextFileNumber();
        pendingOutputs.add(fileNumber);
        mutex.unlock();
        FileMetaData meta;
        try {
            meta = buildTable(mem, fileNumber);
        } finally {
            mutex.lock();
        }
        pendingOutputs.remove(fileNumber);

        // Note that if file size is zero, the file has been deleted and
        // should not be added to the manifest.
        int level = 0;
        if (meta != null && meta.getFileSize() > 0) {
            byte[] minUserKey = meta.getSmallest().getUserKey();
            byte[] maxUserKey = meta.getLargest().getUserKey();
            if (base != null) {
                level = base.pickLevelForMemTableOutput(minUserKey, maxUserKey);
            }
            edit.addFile(level, meta);
        }
    }

    private FileMetaData buildTable(SeekingIterable<InternalKey, byte[]> data, long fileNumber)
            throws IOException {
        File file = new File(databasePath, FileName.tableFileName(fileNumber));
        try {
            InternalKey smallest = null;
            InternalKey largest = null;
            FileChannel channel = new FileOutputStream(file).getChannel();
            try {
                TableBuilder tableBuilder = new TableBuilder(options, channel, new InternalUserComparator(internalKeyComparator));

                for (Map.Entry<InternalKey, byte[]> entry : data) {
                    // update keys
                    InternalKey key = entry.getKey();
                    if (smallest == null) {
                        smallest = key;
                    }
                    largest = key;

                    tableBuilder.add(key.encode(), entry.getValue());
                }

                tableBuilder.finish();
            } finally {
                try {
                    channel.force(true);
                } finally {
                    channel.close();
                }
            }

            if (smallest == null) {
                return null;
            }
            FileMetaData fileMetaData = new FileMetaData(fileNumber, file.length(), smallest, largest);

            // verify table can be opened
            tableCache.newIterator(fileMetaData);

            pendingOutputs.remove(fileNumber);

            return fileMetaData;

        } catch (IOException e) {
            file.delete();
            throw e;
        }
    }

    private void doCompactionWork(CompactionState compactionState)
            throws IOException {
        Preconditions.checkState(mutex.isHeldByCurrentThread());
        Preconditions.checkArgument(versions.numberOfBytesInLevel(compactionState.getCompaction().getLevel()) > 0);
        Preconditions.checkArgument(compactionState.builder == null);
        Preconditions.checkArgument(compactionState.outfile == null);

        // todo track snapshots
        compactionState.smallestSnapshot = versions.getLastSequence();

        // Release mutex while we're actually doing the compaction work
        mutex.unlock();
        try {
            MergingIterator iterator = versions.makeInputIterator(compactionState.compaction);

            byte[] currentUserKey = null;
            boolean hasCurrentUserKey = false;

            long lastSequenceForKey = MAX_SEQUENCE_NUMBER;
            while (iterator.hasNext() && !shuttingDown.get()) {
                // always give priority to compacting the current mem table
                mutex.lock();
                try {
                    compactMemTableInternal();
                } finally {
                    mutex.unlock();
                }

                InternalKey key = iterator.peek().getKey();
                if (compactionState.compaction.shouldStopBefore(key) && compactionState.builder != null) {
                    finishCompactionOutputFile(compactionState);
                }

                // Handle key/value, add to state, etc.
                boolean drop = false;
                // todo if key doesn't parse (it is corrupted),
                if (false /*!ParseInternalKey(key, &ikey)*/) {
                    // do not hide error keys
                    currentUserKey = null;
                    hasCurrentUserKey = false;
                    lastSequenceForKey = MAX_SEQUENCE_NUMBER;
                } else {
                    if (!hasCurrentUserKey || internalKeyComparator.getUserComparator().compare(key.getUserKey(), currentUserKey) != 0) {
                        // First occurrence of this user key
                        currentUserKey = key.getUserKey();
                        hasCurrentUserKey = true;
                        lastSequenceForKey = MAX_SEQUENCE_NUMBER;
                    }

                    if (lastSequenceForKey <= compactionState.smallestSnapshot) {
                        // Hidden by an newer entry for same user key
                        drop = true; // (A)
                    } else if (key.getValueType() == DELETION &&
                            key.getTimestamp() <= compactionState.smallestSnapshot &&
                            compactionState.compaction.isBaseLevelForKey(key.getUserKey())) {
                        // For this user key:
                        // (1) there is no data in higher levels
                        // (2) data in lower levels will have larger sequence numbers
                        // (3) data in layers that are being compacted here and have
                        //     smaller sequence numbers will be dropped in the next
                        //     few iterations of this loop (by rule (A) above).
                        // Therefore this deletion marker is obsolete and can be dropped.
                        drop = true;
                    }

                    lastSequenceForKey = key.getTimestamp();
                }

                if (!drop) {
                    // Open output file if necessary
                    if (compactionState.builder == null) {
                        openCompactionOutputFile(compactionState);
                    }
                    if (compactionState.builder.getEntryCount() == 0) {
                        compactionState.currentSmallest = key;
                    }
                    compactionState.currentLargest = key;
                    compactionState.builder.add(key.encode(), iterator.peek().getValue());

                    // Close output file if it is big enough
                    if (compactionState.builder.getFileSize() >=
                            compactionState.compaction.getMaxOutputFileSize()) {
                        finishCompactionOutputFile(compactionState);
                    }
                }
                iterator.next();
            }

            if (shuttingDown.get()) {
                throw new DatabaseShutdownException("DB shutdown during compaction");
            }
            if (compactionState.builder != null) {
                finishCompactionOutputFile(compactionState);
            }
        } finally {
            mutex.lock();
        }

        // todo port CompactionStats code

        installCompactionResults(compactionState);
    }

    private void openCompactionOutputFile(CompactionState compactionState)
            throws FileNotFoundException {
        Preconditions.checkNotNull(compactionState, "compactionState is null");
        Preconditions.checkArgument(compactionState.builder == null, "compactionState builder is not null");

        mutex.lock();
        try {
            long fileNumber = versions.getNextFileNumber();
            pendingOutputs.add(fileNumber);
            compactionState.currentFileNumber = fileNumber;
            compactionState.currentFileSize = 0;
            compactionState.currentSmallest = null;
            compactionState.currentLargest = null;

            File file = new File(databasePath, FileName.tableFileName(fileNumber));
            compactionState.outfile = new FileOutputStream(file).getChannel();
            compactionState.builder = new TableBuilder(options, compactionState.outfile, new InternalUserComparator(internalKeyComparator));
        } finally {
            mutex.unlock();
        }
    }

    private void finishCompactionOutputFile(CompactionState compactionState)
            throws IOException {
        Preconditions.checkNotNull(compactionState, "compactionState is null");
        Preconditions.checkArgument(compactionState.outfile != null);
        Preconditions.checkArgument(compactionState.builder != null);

        long outputNumber = compactionState.currentFileNumber;
        Preconditions.checkArgument(outputNumber != 0);

        long currentEntries = compactionState.builder.getEntryCount();
        compactionState.builder.finish();

        long currentBytes = compactionState.builder.getFileSize();
        compactionState.currentFileSize = currentBytes;
        compactionState.totalBytes += currentBytes;

        FileMetaData currentFileMetaData = new FileMetaData(compactionState.currentFileNumber,
                compactionState.currentFileSize,
                compactionState.currentSmallest,
                compactionState.currentLargest);
        compactionState.outputs.add(currentFileMetaData);

        compactionState.builder = null;

        compactionState.outfile.force(true);
        compactionState.outfile.close();
        compactionState.outfile = null;

        if (currentEntries > 0) {
            // Verify that the table is usable
            tableCache.newIterator(outputNumber);
        }
    }


    private void installCompactionResults(CompactionState compact)
            throws IOException {
        Preconditions.checkState(mutex.isHeldByCurrentThread());

        // Add compaction outputs
        compact.compaction.addInputDeletions(compact.compaction.getEdit());
        int level = compact.compaction.getLevel();
        for (FileMetaData output : compact.outputs) {
            compact.compaction.getEdit().addFile(level + 1, output);
            pendingOutputs.remove(output.getNumber());
        }

        try {
            versions.logAndApply(compact.compaction.getEdit());
            deleteObsoleteFiles();
        } catch (IOException e) {
            // Compaction failed for some reason.  Simply discard the work and try again later.

            // Discard any files we may have created during this failed compaction
            for (FileMetaData output : compact.outputs) {
                File file = new File(databasePath, FileName.tableFileName(output.getNumber()));
                file.delete();
            }
            compact.outputs.clear();
        }
    }

    int numberOfFilesInLevel(int level) {
        return versions.getCurrent().numberOfFilesInLevel(level);
    }

    public long[] getApproximateSizes(Range... ranges) {
        Preconditions.checkNotNull(ranges, "ranges is null");
        long[] sizes = new long[ranges.length];
        for (int i = 0; i < ranges.length; i++) {
            Range range = ranges[i];
            sizes[i] = getApproximateSizes(range);
        }
        return sizes;
    }

    public long getApproximateSizes(Range range) {
        Version v = versions.getCurrent();

        InternalKey startKey = new InternalKey(range.start(), MAX_SEQUENCE_NUMBER, VALUE);
        InternalKey limitKey = new InternalKey(range.limit(), MAX_SEQUENCE_NUMBER, VALUE);
        long startOffset = v.getApproximateOffsetOf(startKey);
        long limitOffset = v.getApproximateOffsetOf(limitKey);

        return (limitOffset >= startOffset ? limitOffset - startOffset : 0);
    }

    public long getMaxNextLevelOverlappingBytes() {
        return versions.getMaxNextLevelOverlappingBytes();
    }

    private WriteBatch readWriteBatch(ByteBuffer record, int updateSize)
            throws IOException {
        WriteBatch writeBatch = new WriteBatch();
        int entries = 0;
        while (record.hasRemaining()) {
            entries++;
            ValueType valueType = getValueTypeByCode(record.get());
            if (valueType == VALUE) {
                writeBatch.put(Bytes.readLengthPrefixedBytes(record), Bytes.readLengthPrefixedBytes(record));
            } else if (valueType == DELETION) {
                writeBatch.delete(Bytes.readLengthPrefixedBytes(record));
            } else {
                throw new IllegalStateException("Unexpected value type " + valueType);
            }
        }

        if (entries != updateSize) {
            throw new IOException(String.format("Expected %d entries in log record but found %s entries", updateSize, entries));
        }

        return writeBatch;
    }

    private ByteBuffer writeWriteBatch(WriteBatch updates, long sequenceBegin) {
        final ByteBuffer record = ByteBuffer.allocate(RECORD_HEADER_SIZE + updates.getApproximateSize());
        record.putLong(sequenceBegin);
        record.putInt(updates.size());
        updates.forEach(new WriteBatch.Handler() {
            @Override
            public void put(byte[] key, byte[] value) {
                record.put(VALUE.code());
                Bytes.writeLengthPrefixedBytes(record, key);
                Bytes.writeLengthPrefixedBytes(record, value);
            }

            @Override
            public void delete(byte[] key) {
                record.put(DELETION.code());
                Bytes.writeLengthPrefixedBytes(record, key);
            }
        });
        return (ByteBuffer) record.flip();
    }

    private static class CompactionState {
        private final Compaction compaction;

        private final List<FileMetaData> outputs = newArrayList();

        private long smallestSnapshot;

        // State kept for output being generated
        private FileChannel outfile;
        private TableBuilder builder;

        // Current file being generated
        private long currentFileNumber;
        private long currentFileSize;
        private InternalKey currentSmallest;
        private InternalKey currentLargest;

        private long totalBytes;

        private CompactionState(Compaction compaction) {
            this.compaction = compaction;
        }

        public Compaction getCompaction() {
            return compaction;
        }
    }

    private static class ManualCompaction {
        private final int level;
        private final byte[] start;
        private final byte[] end;

        private ManualCompaction(int level, byte[] start, byte[] end) {
            this.level = level;
            this.start = start;
            this.end = end;
        }
    }

    private static class InsertIntoHandler implements WriteBatch.Handler {
        private long sequence;
        private final MemTable memTable;

        public InsertIntoHandler(MemTable memTable, long sequenceBegin) {
            this.memTable = memTable;
            this.sequence = sequenceBegin;
        }

        @Override
        public void put(byte[] key, byte[] value) {
            memTable.add(key, value, sequence++, VALUE);
        }

        @Override
        public void delete(byte[] key) {
            memTable.add(key, DBConstants.EMPTY_BYTE_ARRAY, sequence++, DELETION);
        }
    }

    public static class DatabaseShutdownException extends RacksDBException {
        public DatabaseShutdownException(String message) {
            super(message);
        }
    }

    public static class BackgroundProcessingException extends RacksDBException {
        public BackgroundProcessingException(Throwable cause) {
            super(cause);
        }
    }

}
