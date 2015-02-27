package com.lindb.rocks.table;

import com.lindb.rocks.util.Bytes;

import java.nio.ByteBuffer;
import java.util.Map.Entry;

public enum VersionEditTag {
    COMPARATOR((byte) 1) {
        @Override
        public void readValue(ByteBuffer input, VersionEdit versionEdit) {
            versionEdit.setComparatorName(Bytes.toString(Bytes.readLengthPrefixedBytes(input)));
        }

        @Override
        public void writeValue(ByteBuffer output, VersionEdit versionEdit) {
            String comparatorName = versionEdit.getComparatorName();
            if (comparatorName != null) {
                output.put(code());
                Bytes.writeLengthPrefixedBytes(output, Bytes.toBytes(comparatorName));
            }
        }
    },
    LOG_NUMBER((byte)2) {
        @Override
        public void readValue(ByteBuffer input, VersionEdit versionEdit) {
            versionEdit.setLogNumber(input.getLong());
        }

        @Override
        public void writeValue(ByteBuffer output, VersionEdit versionEdit) {
            Long logNumber = versionEdit.getLogNumber();
            if (logNumber != null) {
                output.put(code());
                output.putLong(logNumber);
            }
        }
    },

    PREVIOUS_LOG_NUMBER((byte)3) {
        @Override
        public void readValue(ByteBuffer input, VersionEdit versionEdit) {
            versionEdit.setPreviousLogNumber(input.getLong());
        }

        @Override
        public void writeValue(ByteBuffer output, VersionEdit versionEdit) {
            Long previousLogNumber = versionEdit.getPreviousLogNumber();
            if (previousLogNumber != null) {
                output.put(code());
                output.putLong(previousLogNumber);
            }
        }
    },

    NEXT_FILE_NUMBER((byte)4) {
        @Override
        public void readValue(ByteBuffer input, VersionEdit versionEdit) {
            versionEdit.setNextFileNumber(input.getLong());
        }

        @Override
        public void writeValue(ByteBuffer output, VersionEdit versionEdit) {
            Long nextFileNumber = versionEdit.getNextFileNumber();
            if (nextFileNumber != null) {
                output.put(code());
                output.putLong(nextFileNumber);
            }
        }
    },

    LAST_SEQUENCE((byte)5) {
        @Override
        public void readValue(ByteBuffer input, VersionEdit versionEdit) {
            versionEdit.setLastSequenceNumber(input.getLong());
        }

        @Override
        public void writeValue(ByteBuffer output, VersionEdit versionEdit) {
            Long lastSequenceNumber = versionEdit.getLastSequenceNumber();
            if (lastSequenceNumber != null) {
                output.put(code());
                output.putLong(lastSequenceNumber);
            }
        }
    },

    COMPACT_POINTER((byte)6) {
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
                output.put(code());

                // level
                output.putInt(entry.getKey());

                // internal key
                Bytes.writeLengthPrefixedBytes(output, entry.getValue().encode());
            }
        }
    },

    DELETED_FILE((byte)7) {
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
                output.put(code());

                // level
                output.putInt(entry.getKey());

                // file number
                output.putLong(entry.getValue());
            }
        }
    },

    NEW_FILE((byte)8) {
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
                output.put(code());

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

    public static VersionEditTag getTypeByCode(byte code) {
        switch (code) {
            case 1:
                return COMPARATOR;
            case 2:
                return LOG_NUMBER;
            case 3:
                return PREVIOUS_LOG_NUMBER;
            case 4:
                return NEXT_FILE_NUMBER;
            case 5:
                return LAST_SEQUENCE;
            case 6:
                return COMPACT_POINTER;
            case 7:
                return DELETED_FILE;
            case 8:
                return NEW_FILE;
            default:
                throw new IllegalArgumentException(String.format("Unknown %s code %d", VersionEditTag.class.getSimpleName(), code));
        }
    }

    private final byte code;

    VersionEditTag(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public abstract void readValue(ByteBuffer input, VersionEdit versionEdit);

    public abstract void writeValue(ByteBuffer output, VersionEdit versionEdit);
}
