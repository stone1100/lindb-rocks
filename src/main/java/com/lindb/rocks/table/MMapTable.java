package com.lindb.rocks.table;

import com.google.common.base.Preconditions;
import com.lindb.rocks.CompressionType;
import com.lindb.rocks.RacksDBException;
import com.lindb.rocks.io.Block.Reader;
import com.lindb.rocks.io.BlockIterator;
import com.lindb.rocks.io.BlockMeta;
import com.lindb.rocks.io.BlockTrailer;
import com.lindb.rocks.io.Footer;
import com.lindb.rocks.util.*;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Comparator;
import java.util.concurrent.Callable;

import static com.lindb.rocks.io.BlockTrailer.BLOCK_TRAILER_ENCODED_LENGTH;

/**
 * @author huang_jie
 *         2/9/2015 4:30 PM
 */
public class MMapTable implements SeekingIterable<byte[], byte[]> {
    private final FileChannel fileChannel;
    private final Comparator<byte[]> comparator;
    private final boolean verifyChecksums;
    private final Reader indexBlock;
    private final BlockMeta metaIndexBlockMeta;

    private static ByteBuffer uncompressedScratch = ByteBuffer.allocateDirect(4 * 1024 * 1024);
    private MappedByteBuffer data;

    public MMapTable(FileChannel fileChannel, Comparator<byte[]> comparator, boolean verifyChecksums) throws IOException {
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
        data = fileChannel.map(MapMode.READ_ONLY, 0, size);
        int footerOffset = (int) (size - Footer.FOOTER_ENCODED_LENGTH);
        data.position(footerOffset);
        Footer footer = Footer.readFooter(data.slice());
        data.position(0);
        return footer;
    }

    protected Reader readBlock(BlockMeta blockMeta) throws IOException {
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
            synchronized (MMapTable.class) {
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
        return new Reader(uncompressedBuffer, comparator);
    }

    public Reader openBlock(BlockMeta blockMeta) {
        Reader dataBlock;
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
