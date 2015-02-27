package com.lindb.rocks.table;

import com.lindb.rocks.log.Log;
import com.lindb.rocks.log.LogChunkType;
import com.lindb.rocks.log.LogMonitor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;


public class LogReader {
    private final FileChannel fileChannel;
    private final LogMonitor monitor;
    private final boolean verifyChecksums;
    /**
     * Offset at which to start looking for the first record to return
     */
    private final long initialOffset;

    /**
     * Have we read to the end of the file?
     */
    private boolean eof;

    /**
     * Offset of the first location past the end of buffer.
     */
    private long endOfBufferOffset;
    /**
     * Scratch buffer in which the next record is assembled.
     */
    private final ByteBuffer recordScratch = ByteBuffer.allocate(Log.BLOCK_SIZE);

    /**
     * Scratch buffer for current block.  The currentBlock is sliced off the underlying buffer.
     */
    private final ByteBuffer blockScratch = ByteBuffer.allocate(Log.BLOCK_SIZE);

    /**
     * The current block records are being read from.
     */
    private ByteBuffer currentBlock;

    /**
     * Current chunk which is sliced from the current block.
     */
    private ByteBuffer currentChunk;

    public LogReader(FileChannel fileChannel, LogMonitor monitor, boolean verifyChecksums, long initialOffset) {
        this.fileChannel = fileChannel;
        this.monitor = monitor;
        this.verifyChecksums = verifyChecksums;
        this.initialOffset = initialOffset;
    }

    public ByteBuffer readRecord() {
        recordScratch.clear();

        boolean inFragmentedRecord = false;
        while (true) {
            LogChunkType chunkType = readNextChunk();
            switch (chunkType) {
                case FULL:
                    if (inFragmentedRecord) {
                        reportCorruption(recordScratch.position(), "Partial record without end");
                        // simply return this full block
                    }
                    recordScratch.clear();
//                    currentChunk.flip();
                    return currentChunk;

                case FIRST:
                    if (inFragmentedRecord) {
                        reportCorruption(recordScratch.position(), "Partial record without end");
                        // clear the scratch and start over from this chunk
                        recordScratch.clear();
                    }
                    recordScratch.put(currentChunk);
                    inFragmentedRecord = true;
                    break;

                case MIDDLE:
                    if (!inFragmentedRecord) {
                        reportCorruption(recordScratch.position(), "Missing start of fragmented record");

                        // clear the scratch and skip this chunk
                        recordScratch.clear();
                    } else {
                        recordScratch.put(currentChunk);
                    }
                    break;

                case LAST:
                    if (!inFragmentedRecord) {
                        reportCorruption(recordScratch.position(), "Missing start of fragmented record");

                        // clear the scratch and skip this chunk
                        recordScratch.clear();
                    } else {
                        recordScratch.put(currentChunk);
                        recordScratch.flip();
                        return recordScratch.slice();
                    }
                    break;

                case EOF:
                    if (inFragmentedRecord) {
                        reportCorruption(recordScratch.position(), "Partial record without end");

                        // clear the scratch and return
                        recordScratch.clear();
                    }
                    return null;

                case BAD_CHUNK:
                    if (inFragmentedRecord) {
                        reportCorruption(recordScratch.position(), "Error in middle of record");
                        inFragmentedRecord = false;
                        recordScratch.clear();
                    }
                    break;

                default:
                    int dropSize = currentChunk.capacity();
                    if (inFragmentedRecord) {
                        dropSize += recordScratch.position();
                    }
                    reportCorruption(dropSize, String.format("Unexpected chunk type %s", chunkType));
                    inFragmentedRecord = false;
                    recordScratch.clear();
                    break;
            }
        }
    }

    /**
     * Return type, or one of the preceding special values
     */
    private LogChunkType readNextChunk() {
        // clear the current chunk
        currentChunk = null;

        // read the next block if necessary
        if (currentBlock == null || (currentBlock.remaining() < Log.CHUNK_HEADER_SIZE)) {
            if (!readNextBlock()) {
                if (eof) {
                    return LogChunkType.EOF;
                }
            }
        }

        // parse header
        int expectedChecksum = currentBlock.getInt();
        int length = currentBlock.getShort();
        byte chunkTypeId = currentBlock.get();
        LogChunkType chunkType = LogChunkType.getLogChunkTypeByCode(chunkTypeId);

        // verify length
        if (length > currentBlock.remaining()) {
            int dropSize = currentBlock.remaining() + Log.CHUNK_HEADER_SIZE;
            reportCorruption(dropSize, "Invalid chunk length");
            currentBlock = null;
            return LogChunkType.BAD_CHUNK;
        }

        // skip zero length records
        if (chunkType == LogChunkType.ZERO_TYPE && length == 0) {
            // Skip zero length record without reporting any drops since
            // such records are produced by the writing code.
            currentBlock = null;
            return LogChunkType.BAD_CHUNK;
        }

        // Skip physical record that started before initialOffset
        if (endOfBufferOffset - Log.CHUNK_HEADER_SIZE - length < initialOffset) {
            currentBlock.position(currentBlock.position() + length);
            return LogChunkType.BAD_CHUNK;
        }

        // read the chunk
        byte[] data = new byte[length];
        currentBlock.get(data);
        currentChunk = ByteBuffer.wrap(data);

        if (verifyChecksums) {
            int actualChecksum = Logs.getChunkChecksum(chunkTypeId, currentChunk);
            if (actualChecksum != expectedChecksum) {
                // Drop the rest of the buffer since "length" itself may have
                // been corrupted and if we trust it, we could find some
                // fragment of a real log record that just happens to look
                // like a valid log record.
                int dropSize = currentBlock.remaining() + Log.CHUNK_HEADER_SIZE;
                currentBlock = null;
                reportCorruption(dropSize, "Invalid chunk checksum");
                return LogChunkType.BAD_CHUNK;
            }
        }

        // Skip unknown chunk types
        // Since this comes last so we the, know it is a valid chunk, and is just a type we don't understand
        if (chunkType == LogChunkType.UNKNOWN) {
            reportCorruption(length, String.format("Unknown chunk type %d", chunkType.code()));
            return LogChunkType.BAD_CHUNK;
        }

        return chunkType;
    }

    public boolean readNextBlock() {
        if (eof) {
            return false;
        }

        // clear the block
        blockScratch.clear();

        // read the next full block
        while (blockScratch.hasRemaining()) {
            try {
                int bytesRead = fileChannel.read(blockScratch);
                if (bytesRead < 0) {
                    // no more bytes to read
                    eof = true;
                    break;
                }
                endOfBufferOffset += bytesRead;
            } catch (IOException e) {
                currentBlock = null;
                reportDrop(Log.BLOCK_SIZE, e);
                eof = true;
                return false;
            }

        }
        blockScratch.flip();
        currentBlock = blockScratch.slice();
        return currentBlock.hasRemaining();
    }

    /**
     * Reports corruption to the monitor.
     * The buffer must be updated to remove the dropped bytes prior to invocation.
     */
    private void reportCorruption(long bytes, String reason) {
        if (monitor != null) {
            monitor.corruption(bytes, reason);
        }
    }

    /**
     * Reports dropped bytes to the monitor.
     * The buffer must be updated to remove the dropped bytes prior to invocation.
     */
    private void reportDrop(long bytes, Throwable reason) {
        if (monitor != null) {
            monitor.corruption(bytes, reason);
        }
    }
}
