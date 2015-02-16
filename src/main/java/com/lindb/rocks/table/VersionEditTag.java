package com.lindb.rocks.table;

import com.google.common.base.Charsets;

import java.nio.ByteBuffer;
import java.util.Map.Entry;

public enum VersionEditTag {
    // 8 is no longer used. It was used for large value refs.

    COMPARATOR(1) {
        @Override
        public void readValue(ByteBuffer input, VersionEdit versionEdit) {
            byte[] bytes = new byte[input.getInt()];
            input.get(bytes);
            versionEdit.setComparatorName(new String(bytes, Charsets.UTF_8));
        }

        @Override
        public void writeValue(ByteBuffer output, VersionEdit versionEdit) {
            String comparatorName = versionEdit.getComparatorName();
            if (comparatorName != null) {
                output.putInt(getPersistentId());
                byte[] bytes = comparatorName.getBytes(Charsets.UTF_8);
                output.putInt(bytes.length);
                output.put(bytes);
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
            byte[] data = new byte[input.getInt()];
            input.get(data);
            InternalKey internalKey = new InternalKey(data);

            versionEdit.setCompactPointer(level, internalKey);
        }

        @Override
        public void writeValue(ByteBuffer output, VersionEdit versionEdit) {
            for (Entry<Integer, InternalKey> entry : versionEdit.getCompactPointers().entrySet()) {
                output.putInt(getPersistentId());

                // level
                output.putInt(entry.getKey());

                // internal key
                byte[] data = entry.getValue().encode();
                output.putInt(data.length);
                output.put(data);
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
            byte[] smallest = new byte[input.getInt()];
            input.get(smallest);
            InternalKey smallestKey = new InternalKey(smallest);

            // largest key
            byte[] largest = new byte[input.getInt()];
            input.get(largest);
            InternalKey largestKey = new InternalKey(largest);

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
                byte[] smallest = fileMetaData.getSmallest().encode();
                output.putInt(smallest.length);
                output.put(smallest);

                // largest key
                byte[] largest = fileMetaData.getLargest().encode();
                output.putInt(largest.length);
                output.put(largest);
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
