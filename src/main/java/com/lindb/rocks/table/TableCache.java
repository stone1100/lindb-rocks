package com.lindb.rocks.table;

import com.google.common.base.Preconditions;
import com.google.common.cache.*;
import com.lindb.rocks.FileName;
import com.lindb.rocks.table.Table.Reader;
import com.lindb.rocks.util.CloseableUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutionException;

/**
 * @author huang_jie
 *         2/9/2015 3:26 PM
 */
public class TableCache {
    private final LoadingCache<Long, TableAndFile> cache;
    private final Finalizer<Reader> finalizer = new Finalizer<>(1);

    public TableCache(final File databasePath, int tableCacheSize, final UserComparator userComparator, final boolean verifyChecksums) {
        Preconditions.checkNotNull(databasePath, "databasePath is null");
        cache = CacheBuilder.newBuilder().maximumSize(tableCacheSize)
                .removalListener(new RemovalListener<Long, TableAndFile>() {
                    @Override
                    public void onRemoval(RemovalNotification<Long, TableAndFile> removalNotification) {
                        TableAndFile tableAndFile = removalNotification.getValue();
                        if (tableAndFile != null) {
                            Reader table = tableAndFile.getTable();
                            finalizer.addCleanup(table, table.closer());
                        }
                    }
                }).build(new CacheLoader<Long, TableAndFile>() {
                    @Override
                    public TableAndFile load(Long fileNumber) throws Exception {
                        return new TableAndFile(databasePath, fileNumber, userComparator, verifyChecksums);
                    }
                });
    }

    public InternalTableIterator newIterator(FileMetaData file) {
        return newIterator(file.getNumber());
    }

    public InternalTableIterator newIterator(long number) {
        return new InternalTableIterator(getTable(number).iterator());
    }

    public long getApproximateOffsetOf(FileMetaData file, byte[] key) {
        return getTable(file.getNumber()).getApproximateOffsetOf(key);
    }

    private Reader getTable(long number) {
        Reader table;
        try {
            table = cache.get(number).getTable();
        } catch (ExecutionException e) {
            Throwable cause = e;
            if (e.getCause() != null) {
                cause = e.getCause();
            }
            throw new RuntimeException("Could not open table " + number, cause);
        }
        return table;
    }

    public void close() {
        cache.invalidateAll();
        finalizer.destroy();
    }

    public void evict(long number) {
        cache.invalidate(number);
    }

    private static final class TableAndFile {
        private final Reader table;
        private final FileChannel fileChannel;

        private TableAndFile(File databasePath, long fileNumber, UserComparator userComparator, boolean verifyChecksums) throws IOException {
            String tableFileName = FileName.tableFileName(fileNumber);
            File tableFile = new File(databasePath, tableFileName);
            fileChannel = new FileInputStream(tableFile).getChannel();
            try {
                table = new Reader(fileChannel, userComparator, verifyChecksums);
            } catch (IOException e) {
                CloseableUtil.close(fileChannel);
                throw e;
            }
        }

        public Reader getTable() {
            return table;
        }
    }
}
