package com.lindb.rocks.table;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.lindb.rocks.CompressionType;
import com.lindb.rocks.DBConstants;
import com.lindb.rocks.Options;
import com.lindb.rocks.RacksDBException;
import com.lindb.rocks.io.*;
import com.lindb.rocks.util.*;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.concurrent.Callable;

import static com.lindb.rocks.io.BlockTrailer.BLOCK_TRAILER_ENCODED_LENGTH;

/**
 * @author huang_jie
 *         2/27/2015 5:34 PM
 */
public class Table {
    public static class Writer {
        public static final long TABLE_MAGIC_NUMBER = 0xdb4775248b80fb57L;

        private final int blockRestartInterval;
        private final int blockSize;
        private final CompressionType compressionType;

        private final FileChannel fileChannel;
        private final Block.Writer dataBlockBuilder;
        private final Block.Writer indexBlockBuilder;
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

        public Writer(Options options, FileChannel fileChannel, UserComparator userComparator) {
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

            dataBlockBuilder = new Block.Writer(blockRestartInterval, userComparator);

            // with expected 50% compression
            indexBlockBuilder = new Block.Writer(1, userComparator);

            lastKey = DBConstants.EMPTY_BYTE_ARRAY;
        }

        public long getEntryCount() {
            return entryCount;
        }

        public long getFileSize() throws IOException {
            return position + dataBlockBuilder.currentSizeEstimate();
        }

        public void add(byte[] key, byte[] value) throws IOException {
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

        private void flush() throws IOException {
            Preconditions.checkState(!closed, "table is finished");
            if (dataBlockBuilder.isEmpty()) {
                return;
            }

            Preconditions.checkState(!pendingIndexEntry, "Internal error: Table already has a pending index entry to flush");

            pendingDataBlockIndex = writeBlock(dataBlockBuilder);
            pendingIndexEntry = true;
        }

        private BlockMeta writeBlock(Block.Writer blockBuilder) throws IOException {
            // close the block
            ByteBuffer raw = blockBuilder.finish();

            // attempt to compress the block
            ByteBuffer blockContents = raw;
            CompressionType blockCompressionType = CompressionType.NONE;
            if (compressionType == CompressionType.SNAPPY) {
                ensureCompressedOutputCapacity(maxCompressedLength(raw.remaining()));
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
            BlockMeta blockMeta = new BlockMeta(position, blockContents.remaining());

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
            Block.Writer metaIndexBlockBuilder = new Block.Writer(blockRestartInterval, new ByteArrayComparator());

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

        public void ensureCompressedOutputCapacity(int capacity) {
            if (compressedOutput != null && compressedOutput.length > capacity) {
                return;
            }
            compressedOutput = new byte[capacity];
        }

        private static int crc32c(ByteBuffer data, CompressionType type) {
            PureJavaCrc32C crc32c = new PureJavaCrc32C();
            crc32c.update(data.array(), data.position(), data.remaining());
            crc32c.update(type.code() & 0xFF);
            return crc32c.getMaskedValue();
        }
    }

    public static class Reader implements SeekingIterable<byte[], byte[]> {
        private final FileChannel fileChannel;
        private final Comparator<byte[]> comparator;
        private final boolean verifyChecksums;
        private final Block.Reader indexBlock;
        private final BlockMeta metaIndexBlockMeta;

        private static ByteBuffer uncompressedScratch = ByteBuffer.allocateDirect(4 * 1024 * 1024);
        private MappedByteBuffer data;

        public Reader(FileChannel fileChannel, Comparator<byte[]> comparator, boolean verifyChecksums) throws IOException {
            long size = fileChannel.size();
            Preconditions.checkArgument(size >= Footer.FOOTER_ENCODED_LENGTH, "File is corrupt: size must be at least %s bytes", Footer.FOOTER_ENCODED_LENGTH);
            Preconditions.checkArgument(size <= Integer.MAX_VALUE, "File must be smaller than %s bytes", Integer.MAX_VALUE);

            this.fileChannel = fileChannel;
            this.verifyChecksums = verifyChecksums;
            this.comparator = comparator;
            Footer footer = init();
            indexBlock = readBlock(footer.getIndexBlockMeta());
            metaIndexBlockMeta = footer.getMetaIndexBlockMeta();
        }

        protected Footer init() throws IOException {
            long size = fileChannel.size();
            data = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size);
            int footerOffset = (int) (size - Footer.FOOTER_ENCODED_LENGTH);
            data.position(footerOffset);
            Footer footer = Footer.readFooter(data.slice());
            data.position(0);
            return footer;
        }

        protected Block.Reader readBlock(BlockMeta blockMeta) throws IOException {
            // read block trailer
            data.position((int) blockMeta.getOffset() + blockMeta.getDataSize());
            int oldLimit = data.limit();
            data.limit((int) blockMeta.getOffset() + blockMeta.getDataSize() + BLOCK_TRAILER_ENCODED_LENGTH);
            BlockTrailer blockTrailer = BlockTrailer.readBlockTrailer(data.slice());
            //reset position and limit
            data.limit(oldLimit);

            data.position((int) blockMeta.getOffset());
            byte[] blockData = new byte[blockMeta.getDataSize()];
            data.get(blockData);

            // only verify check sums if explicitly asked by the user
            if (verifyChecksums) {
                // checksum data and the compression type in the trailer
                PureJavaCrc32C checksum = new PureJavaCrc32C();
                checksum.update(blockData, 0, blockData.length);
                checksum.update(data.get() & 0xFF);
                int actualCrc32c = checksum.getMaskedValue();

                Preconditions.checkState(blockTrailer.getCrc32c() == actualCrc32c, "Block corrupted: checksum mismatch");
            }

            // decompress data
            ByteBuffer uncompressedBuffer = ByteBuffer.allocateDirect(blockData.length);
            uncompressedBuffer.put(blockData);
            uncompressedBuffer.flip();
            if (blockTrailer.getCompressionType() == CompressionType.SNAPPY) {
                synchronized (Reader.class) {
                    int uncompressedLength = uncompressedLength(uncompressedBuffer.duplicate());
                    if (uncompressedScratch.capacity() < uncompressedLength) {
                        uncompressedScratch = ByteBuffer.allocateDirect(uncompressedLength);
                    }
                    uncompressedScratch.clear();

                    Snappy.uncompress(uncompressedBuffer, uncompressedScratch);

                    byte[] t = new byte[uncompressedScratch.remaining()];
                    uncompressedScratch.get(t);
                    uncompressedBuffer = ByteBuffer.wrap(t);
                    uncompressedScratch.clear();
                }
            }
            return new Block.Reader(uncompressedBuffer, comparator);
        }

        public Block.Reader openBlock(BlockMeta blockMeta) {
            Block.Reader dataBlock;
            try {
                dataBlock = readBlock(blockMeta);
            } catch (IOException e) {
                throw new RacksDBException("Read block error: ", e);
            }
            return dataBlock;
        }

        @Override
        public TableIterator iterator() {
            return new TableIterator(this, indexBlock.iterator());
        }

        protected int uncompressedLength(ByteBuffer data)
                throws IOException {
            return Bytes.readVariableLengthInt(data);
        }

        /**
         * Given a key, return an approximate byte offset in the file where
         * the data for that key begins (or would begin if the key were
         * present in the file).  The returned value is in terms of file
         * bytes, and so includes effects like compression of the underlying data.
         * For example, the approximate offset of the last key in the table will
         * be close to the file length.
         */
        public long getApproximateOffsetOf(byte[] key) {
            BlockIterator iterator = indexBlock.iterator();
            iterator.seek(key);
            if (iterator.hasNext()) {
                BlockMeta blockHandle = BlockMeta.readBlockMeta(iterator.next().getValue());
                return blockHandle.getOffset();
            }

            // key is past the last key in the file.  Approximate the offset
            // by returning the offset of the metaindex block (which is
            // right near the end of the file).
            return metaIndexBlockMeta.getOffset();
        }

        public Callable<?> closer() {
            return new Closer(fileChannel, data);
        }

        private static class Closer implements Callable {
            private final Closeable closeable;
            private final MappedByteBuffer data;

            public Closer(Closeable closeable, MappedByteBuffer data) {
                this.closeable = closeable;
                this.data = data;
            }

            public Void call() {
                ByteBufferSupport.unmap(data);
                CloseableUtil.close(closeable);
                return null;
            }
        }
    }
}
