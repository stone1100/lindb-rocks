package com.lindb.rocks.table;

import com.google.common.collect.*;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author huang_jie
 *         2/10/2015 1:20 PM
 */
public class VersionEdit {
    private String comparatorName;
    private Long logNumber;
    private Long nextFileNumber;
    private Long previousLogNumber;
    private Long lastSequenceNumber;
    private final Map<Integer, InternalKey> compactPointers = Maps.newTreeMap();
    private final Multimap<Integer, FileMetaData> newFiles = ArrayListMultimap.create();
    private final Multimap<Integer, Long> deletedFiles = ArrayListMultimap.create();

    public VersionEdit() {
    }

    public VersionEdit(ByteBuffer data) {
        while (data.hasRemaining()) {
            byte code = data.get();
            VersionEditTag tag = VersionEditTag.getTypeByCode(code);
            tag.readValue(data, this);
        }
    }

    public String getComparatorName() {
        return comparatorName;
    }

    public void setComparatorName(String comparatorName) {
        this.comparatorName = comparatorName;
    }

    public Long getLogNumber() {
        return logNumber;
    }

    public void setLogNumber(long logNumber) {
        this.logNumber = logNumber;
    }

    public Long getNextFileNumber() {
        return nextFileNumber;
    }

    public void setNextFileNumber(long nextFileNumber) {
        this.nextFileNumber = nextFileNumber;
    }

    public Long getPreviousLogNumber() {
        return previousLogNumber;
    }

    public void setPreviousLogNumber(long previousLogNumber) {
        this.previousLogNumber = previousLogNumber;
    }

    public Long getLastSequenceNumber() {
        return lastSequenceNumber;
    }

    public void setLastSequenceNumber(long lastSequenceNumber) {
        this.lastSequenceNumber = lastSequenceNumber;
    }

    public Map<Integer, InternalKey> getCompactPointers() {
        return ImmutableMap.copyOf(compactPointers);
    }

    public void setCompactPointer(int level, InternalKey key) {
        compactPointers.put(level, key);
    }

    public void setCompactPointers(Map<Integer, InternalKey> compactPointers) {
        this.compactPointers.putAll(compactPointers);
    }

    public Multimap<Integer, FileMetaData> getNewFiles() {
        return ImmutableMultimap.copyOf(newFiles);
    }

    // Add the specified file at the specified level.
    // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
    // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
    public void addFile(int level, long fileNumber, long fileSize, InternalKey smallest, InternalKey largest) {
        FileMetaData fileMetaData = new FileMetaData(fileNumber, fileSize, smallest, largest);
        addFile(level, fileMetaData);
    }

    public void addFile(int level, FileMetaData fileMetaData) {
        newFiles.put(level, fileMetaData);
    }

    public void addFiles(Multimap<Integer, FileMetaData> files) {
        newFiles.putAll(files);
    }

    public Multimap<Integer, Long> getDeletedFiles() {
        return ImmutableMultimap.copyOf(deletedFiles);
    }

    // Delete the specified "file" from the specified "level".
    public void deleteFile(int level, long fileNumber) {
        deletedFiles.put(level, fileNumber);
    }

    public ByteBuffer encode() {
        ByteBuffer data = ByteBuffer.allocate(4096);
        for (VersionEditTag versionEditTag : VersionEditTag.values()) {
            versionEditTag.writeValue(data, this);
        }
        data.flip();
        return data;
    }

}
