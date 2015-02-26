package com.lindb.rocks.table;

import com.google.common.base.Preconditions;
import com.lindb.rocks.util.ByteBufferSupport;
import com.lindb.rocks.util.CloseableUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicBoolean;

public class MMapLogWriter {
    private static final int PAGE_SIZE = 1024 * 1024;

    private final File file;
    private final long fileNumber;
    private final FileChannel fileChannel;
    private final AtomicBoolean closed = new AtomicBoolean();
    private MappedByteBuffer mappedByteBuffer;
    private long fileOffset;
    /**
     * Current offset in the current block
     */
    private int blockOffset;

    public MMapLogWriter(File file, long fileNumber)
            throws IOException {
        Preconditions.checkNotNull(file, "file is null");
        Preconditions.checkArgument(fileNumber >= 0, "fileNumber is negative");
        this.file = file;
        this.fileNumber = fileNumber;
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, PAGE_SIZE);
    }

    public synchronized void close()
            throws IOException {
        closed.set(true);

        destroyMappedByteBuffer();

        if (fileChannel.isOpen()) {
            fileChannel.truncate(fileOffset);
        }

        // close the channel
        CloseableUtil.close(fileChannel);
    }

    public synchronized void delete()
            throws IOException {
        close();

        // try to delete the file
        file.delete();
    }

    private void destroyMappedByteBuffer() {
        if (mappedByteBuffer != null) {
            fileOffset += mappedByteBuffer.position();
            unmap();
        }
        mappedByteBuffer = null;
    }

    public File getFile() {
        return file;
    }

    public long getFileNumber() {
        return fileNumber;
    }

    // Writes a stream of chunks such that no chunk is split across a block boundary
    public synchronized void addRecord(ByteBuffer record, boolean force)
            throws IOException {
        Preconditions.checkState(!closed.get(), "Log has been closed");

        // used to track first, middle and last blocks
        boolean begin = true;

        // Fragment the record int chunks as necessary and write it.  Note that if record
        // is empty, we still want to iterate once to write a single
        // zero-length chunk.
        do {
            int bytesRemainingInBlock = LogConstants.BLOCK_SIZE - blockOffset;
            Preconditions.checkState(bytesRemainingInBlock >= 0);

            // Switch to a new block if necessary
            if (bytesRemainingInBlock < LogConstants.CHUNK_HEADER_SIZE) {
                if (bytesRemainingInBlock > 0) {
                    // Fill the rest of the block with zeros
                    // todo lame... need a better way to write zeros
                    ensureCapacity(bytesRemainingInBlock);
                    mappedByteBuffer.put(new byte[bytesRemainingInBlock]);
                }
                blockOffset = 0;
                bytesRemainingInBlock = LogConstants.BLOCK_SIZE - blockOffset;
            }

            // Invariant: we never leave less than CHUNK_HEADER_SIZE bytes available in a block
            int bytesAvailableInBlock = bytesRemainingInBlock - LogConstants.CHUNK_HEADER_SIZE;
            Preconditions.checkState(bytesAvailableInBlock >= 0);

            // if there are more bytes in the record then there are available in the block,
            // fragment the record; otherwise write to the end of the record
            boolean end;
            int fragmentLength;
            int available = record.limit() - record.position();
            if (available > bytesAvailableInBlock) {
                end = false;
                fragmentLength = bytesAvailableInBlock;
            } else {
                end = true;
                fragmentLength = available;
            }

            // determine block type
            LogChunkType type;
            if (begin && end) {
                type = LogChunkType.FULL;
            } else if (begin) {
                type = LogChunkType.FIRST;
            } else if (end) {
                type = LogChunkType.LAST;
            } else {
                type = LogChunkType.MIDDLE;
            }

            // write the chunk
            byte[] data = new byte[fragmentLength];
            record.get(data);
            writeChunk(type, ByteBuffer.wrap(data));

            // we are no longer on the first chunk
            begin = false;
        } while (record.remaining() > 0);

        if (force) {
            mappedByteBuffer.force();
        }
    }

    private void writeChunk(LogChunkType type, ByteBuffer record) throws IOException {
        int size = record.remaining();
        Preconditions.checkArgument(size <= 0xffff, "length %s is larger than two bytes", size);
        Preconditions.checkArgument(blockOffset + LogConstants.CHUNK_HEADER_SIZE <= LogConstants.BLOCK_SIZE);

        // create header
        ByteBuffer header = newLogRecordHeader(type, record);

        // write the header and the payload
        ensureCapacity(LogConstants.CHUNK_HEADER_SIZE + record.remaining());
        mappedByteBuffer.put(header);
        mappedByteBuffer.put(record);

        blockOffset += LogConstants.CHUNK_HEADER_SIZE + size;
    }

    private void ensureCapacity(int bytes)
            throws IOException {
        if (mappedByteBuffer.remaining() < bytes) {
            // remap
            fileOffset += mappedByteBuffer.position();
            unmap();

            mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, fileOffset, PAGE_SIZE);
        }
    }

    private void unmap() {
        ByteBufferSupport.unmap(mappedByteBuffer);
    }

    private static ByteBuffer newLogRecordHeader(LogChunkType type, ByteBuffer record) {
        int crc = Logs.getChunkChecksum(type.code(), record.array(), record.position(), record.remaining());

        // Format the header
        ByteBuffer header = ByteBuffer.allocate(LogConstants.CHUNK_HEADER_SIZE);
        header.putInt(crc);
        header.putShort((short) record.remaining());
        header.put(type.code());

        header.flip();
        return header;
    }
}
