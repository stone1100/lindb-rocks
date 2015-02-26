package com.lindb.rocks.table;

import com.lindb.rocks.util.Bytes;

import java.nio.ByteBuffer;
import java.util.Map.Entry;

public enum VersionEditTag {
    // 8 is no longer used. It was used for large value refs.

    COMPARATOR(1) {
        @Override
        public void readValue(ByteBuffer input, VersionEdit versionEdit) {
            versionEdit.setComparatorName(Bytes.toString(Bytes.readLengthPrefixedBytes(input)));
        }

        @Override
        public void writeValue(ByteBuffer output, VersionEdit versionEdit) {
            String comparatorName = versionEdit.getComparatorName();
            if (comparatorName != null) {
                output.putInt(getPersistentId());
                Bytes.writeLengthPrefixedBytes(output, Bytes.toBytes(comparatorName));
            }
        }
    },
    LOG_NUMBER(2) {
        @Override
        public void readValue(ByteBuffer input, VersionEdit versionEdit) {
            versionEdit.setLogNumber(input.getLong());
        }

        @Override
        public void writeValue(ByteBuffer output, VersionEdit versionEdit) {
            Long logNumber = versionEdit.getLogNumber();
            if (logNumber != null) {
                output.putInt(getPersistentId());
                output.putLong(logNumber);
            }
        }
    },

    PREVIOUS_LOG_NUMBER(9) {
        @Override
        public void readValue(ByteBuffer input, VersionEdit versionEdit) {
            versionEdit.setPreviousLogNumber(input.getLong());
        }

        @Override
        public void writeValue(ByteBuffer output, VersionEdit versionEdit) {
            Long previousLogNumber = versionEdit.getPreviousLogNumber();
            if (previousLogNumber != null) {
                output.putInt(getPersistentId());
                output.putLong(previousLogNumber);
            }
        }
    },

    NEXT_FILE_NUMBER(3) {
        @Override
        public void readValue(ByteBuffer input, VersionEdit versionEdit) {
            versionEdit.setNextFileNumber(input.getLong());
        }

        @Override
        public void writeValue(ByteBuffer output, VersionEdit versionEdit) {
            Long nextFileNumber = versionEdit.getNextFileNumber();
            if (nextFileNumber != null) {
                output.putInt(getPersistentId());
                output.putLong(nextFileNumber);
            }
        }
    },

    LAST_SEQUENCE(4) {
        @Override
        public void readValue(ByteBuffer input, VersionEdit versionEdit) {
            versionEdit.setLastSequenceNumber(input.getLong());
        }

        @Override
        public void writeValue(ByteBuffer output, VersionEdit versionEdit) {
            Long lastSequenceNumber = versionEdit.getLastSequenceNumber();
            if (lastSequenceNumber != null) {
                output.putInt(getPersistentId());
                output.putLong(lastSequenceNumber);
            }
        }
    },

    COMPACT_POINTER(5) {
        @Override
        public void readValue(ByteBuffer input, VersionEdit versionEdit) {
            // level
            int level = input.getInt();

            // internal key
            InternalKey internalKey = new InternalKey(Bytes.readLengthPrefixedBytes(input));

            versionEdit.setCompactPointer(level, internalKey);
        }

        @Override
        public void writeValue(ByteBuffer output, VersionEdit versionEdit) {
            for (Entry<Integer, InternalKey> entry : versionEdit.getCompactPointers().entrySet()) {
                output.putInt(getPersistentId());

                // level
                output.putInt(entry.getKey());

                // internal key
                Bytes.writeLengthPrefixedBytes(output, entry.getValue().encode());
            }
        }
    },

    DELETED_FILE(6) {
        @Override
        public void readValue(ByteBuffer input, VersionEdit versionEdit) {
            // level
            int level = input.getInt();

            // file number
            long fileNumber = input.getLong();

            versionEdit.deleteFile(level, fileNumber);
        }

        @Override
        public void writeValue(ByteBuffer output, VersionEdit versionEdit) {
            for (Entry<Integer, Long> entry : versionEdit.getDeletedFiles().entries()) {
                output.putInt(getPersistentId());

                // level
                output.putInt(entry.getKey());

                // file number
                output.putLong(entry.getValue());
            }
        }
    },

    NEW_FILE(7) {
        @Override
        public void readValue(ByteBuffer input, VersionEdit versionEdit) {
            // level
            int level = input.getInt();

            // file number
            long fileNumber = input.getLong();

            // file size
            long fileSize = input.getLong();

            // smallest key
            InternalKey smallestKey = new InternalKey(Bytes.readLengthPrefixedBytes(input));

            // largest key
            InternalKey largestKey = new InternalKey(Bytes.readLengthPrefixedBytes(input));

            versionEdit.addFile(level, fileNumber, fileSize, smallestKey, largestKey);
        }

        @Override
        public void writeValue(ByteBuffer output, VersionEdit versionEdit) {
            for (Entry<Integer, FileMetaData> entry : versionEdit.getNewFiles().entries()) {
                output.putInt(getPersistentId());

                // level
                output.putInt(entry.getKey());

                // file number
                FileMetaData fileMetaData = entry.getValue();
                output.putLong(fileMetaData.getNumber());

                // file size
                output.putLong(fileMetaData.getFileSize());

                // smallest key
                Bytes.writeLengthPrefixedBytes(output, fileMetaData.getSmallest().encode());

                // largest key
                Bytes.writeLengthPrefixedBytes(output, fileMetaData.getLargest().encode());
            }
        }
    };

    public static VersionEditTag getValueTypeByPersistentId(int persistentId) {
        for (VersionEditTag compressionType : VersionEditTag.values()) {
            if (compressionType.persistentId == persistentId) {
                return compressionType;
            }
        }
        throw new IllegalArgumentException(String.format("Unknown %s persistentId %d", VersionEditTag.class.getSimpleName(), persistentId));
    }

    private final int persistentId;

    VersionEditTag(int persistentId) {
        this.persistentId = persistentId;
    }

    public int getPersistentId() {
        return persistentId;
    }

    public abstract void readValue(ByteBuffer input, VersionEdit versionEdit);

    public abstract void writeValue(ByteBuffer output, VersionEdit versionEdit);
}
