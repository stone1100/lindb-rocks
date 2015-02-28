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

public class Level implements SeekingIterable<InternalKey, byte[]> {
    private final int levelNumber;
    private final TableCache tableCache;
    private final InternalKeyComparator internalKeyComparator;
    private final List<FileMetaData> files;

    public Level(int levelNumber, List<FileMetaData> files, TableCache tableCache, InternalKeyComparator internalKeyComparator) {
        Preconditions.checkArgument(levelNumber >= 0, "levelNumber is negative");
        Preconditions.checkNotNull(files, "files is null");
        Preconditions.checkNotNull(tableCache, "tableCache is null");
        Preconditions.checkNotNull(internalKeyComparator, "internalKeyComparator is null");

        this.files = newArrayList(files);
        this.tableCache = tableCache;
        this.internalKeyComparator = internalKeyComparator;
        Preconditions.checkArgument(levelNumber >= 0, "levelNumber is negative");
        this.levelNumber = levelNumber;
    }

    public int getLevelNumber() {
        return levelNumber;
    }

    public List<FileMetaData> getFiles() {
        return files;
    }

    @Override
    public LevelIterator iterator() {
        return createLevelConcatIterator(tableCache, files, internalKeyComparator);
    }

    public static LevelIterator createLevelConcatIterator(TableCache tableCache, List<FileMetaData> files, InternalKeyComparator internalKeyComparator) {
        return new LevelIterator(tableCache, files, internalKeyComparator);
    }

    public LookupResult get(LookupKey key, ReadStats readStats) {
        if (files.isEmpty()) {
            return null;
        }

        List<FileMetaData> fileMetaDataList = Lists.newArrayListWithCapacity(files.size());
        if (levelNumber == 0) {
            for (FileMetaData fileMetaData : files) {
                if (internalKeyComparator.getUserComparator().compare(key.getUserKey(), fileMetaData.getSmallest().getUserKey()) >= 0 &&
                        internalKeyComparator.getUserComparator().compare(key.getUserKey(), fileMetaData.getLargest().getUserKey()) <= 0) {
                    fileMetaDataList.add(fileMetaData);
                }
            }
        } else {
            // Binary search to find earliest index whose largest key >= ikey.
            int index = ceilingEntryIndex(Lists.transform(files, FileMetaData.GET_LARGEST_USER_KEY), key.getInternalKey(), internalKeyComparator);

            // did we find any files that could contain the key?
            if (index >= files.size()) {
                return null;
            }

            // check if the smallest user key in the file is less than the target user key
            FileMetaData fileMetaData = files.get(index);
            if (internalKeyComparator.getUserComparator().compare(key.getUserKey(), fileMetaData.getSmallest().getUserKey()) < 0) {
                return null;
            }

            // search this file
            fileMetaDataList.add(fileMetaData);
        }

        FileMetaData lastFileRead = null;
        int lastFileReadLevel = -1;
        readStats.clear();
        for (FileMetaData fileMetaData : fileMetaDataList) {
            if (lastFileRead != null && readStats.getSeekFile() == null) {
                // We have had more than one seek for this read.  Charge the first file.
                readStats.setSeekFile(lastFileRead);
                readStats.setSeekFileLevel(lastFileReadLevel);
            }

            lastFileRead = fileMetaData;
            lastFileReadLevel = levelNumber;

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
                if (key.getUserKey().equals(internalKey.getUserKey())) {
                    if (internalKey.getValueType() == ValueType.DELETION) {
                        return LookupResult.deleted(key);
                    } else if (internalKey.getValueType() == ValueType.VALUE) {
                        return LookupResult.ok(key, entry.getValue());
                    }
                }
            }
        }

        return null;
    }

    private static <T> int ceilingEntryIndex(List<T> list, T key, Comparator<T> comparator) {
        int insertionPoint = Collections.binarySearch(list, key, comparator);
        if (insertionPoint < 0) {
            insertionPoint = -(insertionPoint + 1);
        }
        return insertionPoint;
    }

    public boolean someFileOverlapsRange(byte[] smallestUserKey, byte[] largestUserKey) {
        InternalKey smallestInternalKey = new InternalKey(smallestUserKey, DBConstants.MAX_SEQUENCE_NUMBER, ValueType.VALUE);
        int index = findFile(smallestInternalKey);

        UserComparator userComparator = internalKeyComparator.getUserComparator();
        return ((index < files.size()) &&
                userComparator.compare(largestUserKey, files.get(index).getSmallest().getUserKey()) >= 0);
    }

    private int findFile(InternalKey targetKey) {
        if (files.isEmpty()) {
            return files.size();
        }

        // todo replace with Collections.binarySearch
        int left = 0;
        int right = files.size() - 1;

        // binary search restart positions to find the restart position immediately before the targetKey
        while (left < right) {
            int mid = (left + right) / 2;

            if (internalKeyComparator.compare(files.get(mid).getLargest(), targetKey) < 0) {
                // Key at "mid.largest" is < "target".  Therefore all
                // files at or before "mid" are uninteresting.
                left = mid + 1;
            } else {
                // Key at "mid.largest" is >= "target".  Therefore all files
                // after "mid" are uninteresting.
                right = mid;
            }
        }
        return right;
    }

    public void addFile(FileMetaData fileMetaData) {
        // todo remove mutation
        files.add(fileMetaData);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Level");
        sb.append("{levelNumber=").append(levelNumber);
        sb.append(", files=").append(files);
        sb.append('}');
        return sb.toString();
    }
}
