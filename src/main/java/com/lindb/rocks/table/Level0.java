package com.lindb.rocks.table;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.lindb.rocks.DBConstants;
import com.lindb.rocks.LookupKey;
import com.lindb.rocks.LookupResult;
import com.lindb.rocks.util.Bytes;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

import static com.google.common.collect.Lists.newArrayList;

public class Level0 implements SeekingIterable<InternalKey, byte[]> {
    private final TableCache tableCache;
    private final InternalKeyComparator internalKeyComparator;
    private final List<FileMetaData> files;

    public static final Comparator<FileMetaData> NEWEST_FIRST = new Comparator<FileMetaData>() {
        @Override
        public int compare(FileMetaData fileMetaData, FileMetaData fileMetaData1) {
            return (int) (fileMetaData1.getNumber() - fileMetaData.getNumber());
        }
    };

    public Level0(List<FileMetaData> files, TableCache tableCache, InternalKeyComparator internalKeyComparator) {
        Preconditions.checkNotNull(files, "files is null");
        Preconditions.checkNotNull(tableCache, "tableCache is null");
        Preconditions.checkNotNull(internalKeyComparator, "internalKeyComparator is null");

        this.files = newArrayList(files);
        this.tableCache = tableCache;
        this.internalKeyComparator = internalKeyComparator;
    }

    public int getLevelNumber() {
        return 0;
    }

    public List<FileMetaData> getFiles() {
        return files;
    }

    @Override
    public Level0Iterator iterator() {
        return new Level0Iterator(tableCache, files, internalKeyComparator);
    }

    public LookupResult get(LookupKey key, ReadStats readStats) {
        if (files.isEmpty()) {
            return null;
        }

        List<FileMetaData> fileMetaDataList = Lists.newArrayListWithCapacity(files.size());
        for (FileMetaData fileMetaData : files) {
            if (internalKeyComparator.getUserComparator().compare(key.getUserKey(), fileMetaData.getSmallest().getUserKey()) >= 0 &&
                    internalKeyComparator.getUserComparator().compare(key.getUserKey(), fileMetaData.getLargest().getUserKey()) <= 0) {
                fileMetaDataList.add(fileMetaData);
            }
        }

        Collections.sort(fileMetaDataList, NEWEST_FIRST);

        readStats.clear();
        for (FileMetaData fileMetaData : fileMetaDataList) {
            // open the iterator
            InternalTableIterator iterator = tableCache.newIterator(fileMetaData);

            // seek to the key
            iterator.seek(key.getInternalKey());

            if (iterator.hasNext()) {
                // parse the key in the block
                Entry<InternalKey, byte[]> entry = iterator.next();
                InternalKey internalKey = entry.getKey();
                Preconditions.checkState(internalKey != null, "Corrupt key for %s", Bytes.toStringBinary(key.getUserKey()));

                // if this is a value key (not a delete) and the keys match, return the value
                if (Bytes.compareTo(key.getUserKey(), internalKey.getUserKey()) == 0) {
                    if (internalKey.getValueType() == ValueType.DELETION) {
                        return LookupResult.deleted(key);
                    } else if (internalKey.getValueType() == ValueType.VALUE) {
                        return LookupResult.ok(key, entry.getValue());
                    }
                }
            }

            if (readStats.getSeekFile() == null) {
                // We have had more than one seek for this read.  Charge the first file.
                readStats.setSeekFile(fileMetaData);
                readStats.setSeekFileLevel(0);
            }
        }

        return null;
    }

    public boolean someFileOverlapsRange(byte[] smallestUserKey, byte[] largestUserKey) {
        InternalKey smallestInternalKey = new InternalKey(smallestUserKey, DBConstants.MAX_SEQUENCE_NUMBER, ValueType.VALUE);
        int index = findFile(smallestInternalKey);

        UserComparator userComparator = internalKeyComparator.getUserComparator();
        return ((index < files.size()) && userComparator.compare(largestUserKey, files.get(index).getSmallest().getUserKey()) >= 0);
    }

    private int findFile(InternalKey targetKey) {
        if (files.isEmpty()) {
            return files.size();
        }

        int left = 0;
        int right = files.size() - 1;

        // binary search restart positions to find the restart position immediately before the targetKey
        while (left < right) {
            int mid = (left + right) / 2;

            if (internalKeyComparator.compare(files.get(mid).getLargest(), targetKey) < 0) {
                // Key at "mid.largest" is < "target".  Therefore all files at or before "mid" are uninteresting.
                left = mid + 1;
            } else {
                // Key at "mid.largest" is >= "target".  Therefore all files after "mid" are uninteresting.
                right = mid;
            }
        }
        return right;
    }

    public void addFile(FileMetaData fileMetaData) {
        // todo remove mutation
        files.add(fileMetaData);
    }

}
