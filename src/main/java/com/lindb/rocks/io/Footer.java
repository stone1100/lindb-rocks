package com.lindb.rocks.io;

import com.google.common.base.Preconditions;
import com.lindb.rocks.table.TableBuilder;

import java.nio.ByteBuffer;

import static com.lindb.rocks.io.BlockMeta.BLOCK_META_ENCODED_LENGTH;
import static com.lindb.rocks.util.Bytes.SIZEOF_LONG;

/**
 *
 * @author huang_jie
 *         2/9/2015 4:12 PM
 */
public class Footer {
    public static final int FOOTER_ENCODED_LENGTH = (BLOCK_META_ENCODED_LENGTH * 2) + SIZEOF_LONG;
    private final BlockMeta metaIndexBlockMeta;
    private final BlockMeta indexBlockMeta;

    public Footer(BlockMeta metaIndexBlockMeta, BlockMeta indexBlockMeta) {
        this.metaIndexBlockMeta = metaIndexBlockMeta;
        this.indexBlockMeta = indexBlockMeta;
    }

    public BlockMeta getIndexBlockMeta() {
        return indexBlockMeta;
    }

    public BlockMeta getMetaIndexBlockMeta() {
        return metaIndexBlockMeta;
    }

    public static Footer readFooter(ByteBuffer data) {
        // read meta index and index meta information
        BlockMeta metaIndexBlockHandle = new BlockMeta(data.getLong(), data.getInt());
        BlockMeta indexBlockHandle = new BlockMeta(data.getLong(), data.getInt());

        // verify magic number
        long magicNumber = getUnsignedInt(data.getInt()) | (getUnsignedInt(data.getInt()) << 32);
        Preconditions.checkArgument(magicNumber == TableBuilder.TABLE_MAGIC_NUMBER, "File is not a table (bad magic number)");

        return new Footer(metaIndexBlockHandle, indexBlockHandle);
    }

    public static long getUnsignedInt(int data) {
        return data & 0xFFFFFFFFL;
    }

    public ByteBuffer encode() {
        ByteBuffer data = ByteBuffer.allocate(FOOTER_ENCODED_LENGTH);
        data.put(metaIndexBlockMeta.encode());
        data.put(indexBlockMeta.encode());
        data.putInt((int) TableBuilder.TABLE_MAGIC_NUMBER);
        data.putInt((int) (TableBuilder.TABLE_MAGIC_NUMBER >>> 32));
        return (ByteBuffer)data.flip();
    }
}
