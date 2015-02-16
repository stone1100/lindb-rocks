package com.lindb.rocks.table;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.lindb.rocks.CompressionType;
import com.lindb.rocks.DBConstants;
import com.lindb.rocks.Options;
import com.lindb.rocks.io.BlockBuilder;
import com.lindb.rocks.io.BlockMeta;
import com.lindb.rocks.io.BlockTrailer;
import com.lindb.rocks.io.Footer;
import com.lindb.rocks.util.PureJavaCrc32C;
import com.lindb.rocks.util.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author huang_jie
 *         2/9/2015 4:17 PM
 */
public class TableBuilder {
    public static final long TABLE_MAGIC_NUMBER = 0xdb4775248b80fb57L;

    private final int blockRestartInterval;
    private final int blockSize;
    private final CompressionType compressionType;

    private final FileChannel fileChannel;
    private final BlockBuilder dataBlockBuilder;
    private final BlockBuilder indexBlockBuilder;
    private byte[] lastKey;
    private final UserComparator userComparator;

    private long entryCount;

    // Either Finish() or Abandon() has been called.
    private boolean closed;

    // We do not emit the index entry for a block until we have seen the
    // first key for the next data block.  This allows us to use shorter
    // keys in the index block.  For example, consider a block boundary
    // between the keys "the quick brown fox" and "the who".  We can use
    // "the r" as the key for the index block entry since it is >= all
    // entries in the first block and < all entries in subsequent
    // blocks.
    private boolean pendingIndexEntry;
    private BlockMeta pendingDataBlockIndex;  // Handle to add to index block

    private byte[] compressedOutput;

    private long position;

    public TableBuilder(Options options, FileChannel fileChannel, UserComparator userComparator) {
        Preconditions.checkNotNull(options, "options is null");
        Preconditions.checkNotNull(fileChannel, "fileChannel is null");
        try {
            Preconditions.checkState(position == fileChannel.position(), "Expected position %s to equal fileChannel.position %s", position, fileChannel.position());
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        this.fileChannel = fileChannel;
        this.userComparator = userComparator;

        blockRestartInterval = options.blockRestartInterval();
        blockSize = options.blockSize();
        compressionType = options.compressionType();

        dataBlockBuilder = new BlockBuilder((int) Math.min(blockSize * 1.1, VersionSet.TARGET_FILE_SIZE), blockRestartInterval, userComparator);

        // with expected 50% compression
        int expectedNumberOfBlocks = 1024;
        indexBlockBuilder = new BlockBuilder(BlockMeta.BLOCK_META_ENCODED_LENGTH * expectedNumberOfBlocks, 1, userComparator);

        lastKey = DBConstants.EMPTY_BYTE_ARRAY;
    }

    public long getEntryCount() {
        return entryCount;
    }

    public long getFileSize() throws IOException {
        return position + dataBlockBuilder.currentSizeEstimate();
    }

    public void add(byte[] key, byte[] value) throws IOException {
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(value, "value is null");

        Preconditions.checkState(!closed, "table is finished");

        if (entryCount > 0) {
            assert (userComparator.compare(key, lastKey) > 0) : "key must be greater than last key";
        }

        // If we just wrote a block, we can now add the handle to index block
        if (pendingIndexEntry) {
            Preconditions.checkState(dataBlockBuilder.isEmpty(), "Internal error: Table has a pending index entry but data block builder is empty");

            byte[] shortestSeparator = userComparator.findShortestSeparator(lastKey, key);

            indexBlockBuilder.add(shortestSeparator, pendingDataBlockIndex.encode().array());
            pendingIndexEntry = false;
        }

        lastKey = key;
        entryCount++;
        dataBlockBuilder.add(key, value);

        int estimatedBlockSize = dataBlockBuilder.currentSizeEstimate();
        if (estimatedBlockSize >= blockSize) {
            flush();
        }
    }

    private void flush()
            throws IOException {
        Preconditions.checkState(!closed, "table is finished");
        if (dataBlockBuilder.isEmpty()) {
            return;
        }

        Preconditions.checkState(!pendingIndexEntry, "Internal error: Table already has a pending index entry to flush");

        pendingDataBlockIndex = writeBlock(dataBlockBuilder);
        pendingIndexEntry = true;
    }

    private BlockMeta writeBlock(BlockBuilder blockBuilder) throws IOException {
        // close the block
        ByteBuffer raw = blockBuilder.finish();

        // attempt to compress the block
        ByteBuffer blockContents = raw;
        CompressionType blockCompressionType = CompressionType.NONE;
        if (compressionType == CompressionType.SNAPPY) {
            ensureCompressedOutputCapacity(maxCompressedLength(raw.limit()));
            try {
                int compressedSize = Snappy.compress(raw.array(), raw.position(), raw.remaining(), compressedOutput, 0);

                // Don't use the compressed data if compressed less than 12.5%,
                if (compressedSize < raw.limit() - (raw.limit() / 8)) {
                    blockContents = ByteBuffer.wrap(compressedOutput, 0, compressedSize);
                    blockCompressionType = CompressionType.SNAPPY;
                }
            } catch (IOException ignored) {
                // compression failed, so just store uncompressed form
            }
        }

        // create block trailer
        BlockTrailer blockTrailer = new BlockTrailer(blockCompressionType, crc32c(blockContents, blockCompressionType));

        // create a meta to this block
        BlockMeta blockMeta =  new BlockMeta(position, blockContents.limit());

        // write data and trailer
        position += fileChannel.write(new ByteBuffer[]{blockContents, blockTrailer.encode()});

        // clean up state
        blockBuilder.reset();

        return blockMeta;
    }

    private static int maxCompressedLength(int length) {
        // Compressed data can be defined as:
        //    compressed := item* literal*
        //    item       := literal* copy
        //
        // The trailing literal sequence has a space blowup of at most 62/60
        // since a literal of length 60 needs one tag byte + one extra byte
        // for length information.
        //
        // Item blowup is trickier to measure.  Suppose the "copy" op copies
        // 4 bytes of data.  Because of a special check in the encoding code,
        // we produce a 4-byte copy only if the offset is < 65536.  Therefore
        // the copy op takes 3 bytes to encode, and this type of item leads
        // to at most the 62/60 blowup for representing literals.
        //
        // Suppose the "copy" op copies 5 bytes of data.  If the offset is big
        // enough, it will take 5 bytes to encode the copy op.  Therefore the
        // worst case here is a one-byte literal followed by a five-byte copy.
        // I.e., 6 bytes of input turn into 7 bytes of "compressed" data.
        //
        // This last factor dominates the blowup, so the final estimate is:
        return 32 + length + (length / 6);
    }

    public void finish() throws IOException {
        Preconditions.checkState(!closed, "table is finished");

        // flush current data block
        flush();

        // mark table as closed
        closed = true;

        // write (empty) meta index block
        BlockBuilder metaIndexBlockBuilder = new BlockBuilder(256, blockRestartInterval, new ByteArrayComparator());

        BlockMeta metaIndexBlockMeta = writeBlock(metaIndexBlockBuilder);

        // add last handle to index block
        if (pendingIndexEntry) {
            byte[] shortSuccessor = userComparator.findShortSuccessor(lastKey);

            indexBlockBuilder.add(shortSuccessor, pendingDataBlockIndex.encode().array());
            pendingIndexEntry = false;
        }

        // write index block
        BlockMeta indexBlockMeta = writeBlock(indexBlockBuilder);

        // write footer
        Footer footer = new Footer(metaIndexBlockMeta, indexBlockMeta);
        position += fileChannel.write(footer.encode());
    }

    public void abandon() {
        Preconditions.checkState(!closed, "table is finished");
        closed = true;
    }

    public static int crc32c(ByteBuffer data, CompressionType type) {
        PureJavaCrc32C crc32c = new PureJavaCrc32C();
        crc32c.update(data.array(), data.position(), data.remaining());
        crc32c.update(type.code() & 0xFF);
        return crc32c.getMaskedValue();
    }

    public void ensureCompressedOutputCapacity(int capacity) {
        if (compressedOutput != null && compressedOutput.length > capacity) {
            return;
        }
        compressedOutput = new byte[capacity];
    }
}
