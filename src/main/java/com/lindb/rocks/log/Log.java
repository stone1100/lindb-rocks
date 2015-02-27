package com.lindb.rocks.log;

import com.google.common.base.Preconditions;
import com.lindb.rocks.io.ByteArrayStream;
import com.lindb.rocks.util.ByteBufferSupport;
import com.lindb.rocks.util.CloseableUtil;
import com.lindb.rocks.util.PureJavaCrc32C;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.lindb.rocks.util.Bytes.*;

/**
 * Log related write/read. Using memory map file.
 * Format:
 * File: |page1|page2|page3|...., each page size is PAGE_SIZE
 * Page: |block1|block2|block3|..., each block size is BLOCK_SIZE
 * Block: |chunk header 1|chunk1|....
 * Chunk Header: checksum(4 bytes) + chunk type(1 byte) + length(2 bytes)
 *
 * @author huang_jie
 *         2/27/2015 1:07 PM
 */
public class Log {
    public static final int PAGE_SIZE = 1024 * 1024;//1MB
    public static final int BLOCK_SIZE = 3 * 1024;//3KB
    //Chunk Header is checksum (4 bytes), type (1 byte), length (2 bytes).
    public static final int CHUNK_HEADER_SIZE = SIZEOF_INT + SIZEOF_BYTE + SIZEOF_SHORT;

    public static Writer createWriter(File file, long fileNumber) throws IOException {
        return new Writer(file, fileNumber);
    }

    public static int getChunkChecksum(int chunkTypeId, byte[] buffer, int offset, int length) {
        // Compute the crc of the record type and the payload.
        PureJavaCrc32C crc32C = new PureJavaCrc32C();
        crc32C.update(chunkTypeId);
        crc32C.update(buffer, offset, length);
        return crc32C.getMaskedValue();
    }

    /**
     * Log Writer Implement
     */
    public static class Writer {
        private final File file;
        private final long fileNumber;
        private final FileChannel fileChannel;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private MappedByteBuffer buf;
        //current offset in the current block
        private int blockOffset;
        private int fileOffset;

        public Writer(File file, long fileNumber) throws IOException {
            this.file = file;
            this.fileNumber = fileNumber;
            this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
            map();
        }

        public File getFile() {
            return file;
        }

        public long getFileNumber() {
            return fileNumber;
        }

        /**
         * Write a stream of chunks such that no chunk is split across a block boundary
         */
        public synchronized void addRecord(ByteBuffer record, boolean force) throws IOException {
            if (closed.get()) {
                throw new IllegalStateException("Log writer has been closed");
            }
            //used to trace first, middle and last blocks
            boolean begin = true;
            /*
             * Fragment the record into chunks as necessary and write it.
             * Note that if record is empty, we still want to iterate once to write a single zero-length chunk.
             */
            do {
                int bytesRemainingInBlock = BLOCK_SIZE - blockOffset;
                //switch to a new block if necessary
                if (bytesRemainingInBlock < CHUNK_HEADER_SIZE) {
                    ensureCapacity(bytesRemainingInBlock);
                    //make sure each block size is BLOCK_SIZE in memory map file
                    buf.put(new byte[bytesRemainingInBlock]);
                    blockOffset = 0;
                    bytesRemainingInBlock = BLOCK_SIZE - blockOffset;
                }
                //Note: never leave less than CHUNK_HEADER_SIZE bytes available in a block
                int bytesAvailableInBlock = bytesRemainingInBlock - CHUNK_HEADER_SIZE;
                /*
                 * If there are more bytes in the record then there are available in the block, fragment the record;
                 * otherwise write to the end of the record.
                 */
                boolean end = true;
                int fragmentLength = record.remaining();
                if (fragmentLength > bytesAvailableInBlock) {
                    end = false;
                    fragmentLength = bytesAvailableInBlock;
                }
                //determine chunk type
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

                //write chunk
                byte[] chunk = new byte[fragmentLength];
                record.get(chunk);
                writeChunk(type, chunk);
                //we are no longer on the first chunk
                begin = false;
            } while (record.hasRemaining());

            if (force) {
                buf.force();
            }
        }

        public synchronized void close() throws IOException {
            closed.set(true);
            destroyBuffer();
            if (fileChannel.isOpen()) {
                fileChannel.truncate(fileOffset);
                CloseableUtil.close(fileChannel);
            }
        }

        public synchronized void delete() throws IOException {
            close();
            //try to delete the file
            file.delete();
        }

        private void destroyBuffer() {
            if (buf != null) {
                fileOffset += buf.position();
                unmap();
                buf = null;
            }
        }

        private void writeChunk(LogChunkType type, byte[] chunk) throws IOException {
            int len = chunk.length;
            Preconditions.checkArgument(len <= 0xffff, "length %s is larger than two bytes", len);
            Preconditions.checkArgument(blockOffset + CHUNK_HEADER_SIZE <= BLOCK_SIZE);

            // create header
            ByteBuffer header = newChunkHeader(type, chunk);

            // write header and chunk
            ensureCapacity(CHUNK_HEADER_SIZE + len);
            buf.put(header);
            buf.put(chunk);

            blockOffset += CHUNK_HEADER_SIZE + len;
        }

        private ByteBuffer newChunkHeader(LogChunkType type, byte[] chunk) {
            int crc = getChunkChecksum(type.code(), chunk, 0, chunk.length);
            ByteBuffer header = ByteBuffer.allocate(CHUNK_HEADER_SIZE);
            header.putInt(crc);
            header.putShort((short) chunk.length);
            header.put(type.code());
            header.flip();
            return header;
        }

        private void unmap() {
            ByteBufferSupport.unmap(buf);
        }

        private void map() throws IOException {
            buf = fileChannel.map(FileChannel.MapMode.READ_WRITE, fileOffset, PAGE_SIZE);
        }

        private void ensureCapacity(int size) throws IOException {
            if (buf.remaining() < size) {
                //remap
                fileOffset += buf.position();
                unmap();
                map();
            }
        }
    }

    /**
     * Log reader implement
     */
    public static class Reader {
        private final FileChannel fileChannel;
        private final LogMonitor monitor;
        private final boolean verifyChecksum;
        //Offset at which to start looking for the first record to return
        private final long initialOffset;
        //Scratch buffer in which the next record is assembled.
        private final ByteArrayStream recordScratch = new ByteArrayStream(BLOCK_SIZE);
        //Scratch buffer for current block.
        private final ByteBuffer blockScratch = ByteBuffer.allocate(BLOCK_SIZE);
        //Current block records are being read from.
        private ByteBuffer currentBlock;
        //Current chunk which is sliced from the current block.
        private ByteBuffer currentChunk;

        public Reader(FileChannel fileChannel, LogMonitor monitor, boolean verifyChecksum, long initialOffset) {
            this.fileChannel = fileChannel;
            this.monitor = monitor;
            this.verifyChecksum = verifyChecksum;
            this.initialOffset = initialOffset;
        }

        public ByteBuffer readRecord() {
            recordScratch.reset();
            boolean inFragmentedRecord = false;
            while (true) {

            }
        }

//        private Log
    }

}
