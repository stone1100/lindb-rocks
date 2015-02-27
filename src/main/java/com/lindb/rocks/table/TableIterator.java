package com.lindb.rocks.table;

import com.lindb.rocks.io.Block.Reader;
import com.lindb.rocks.io.BlockIterator;
import com.lindb.rocks.io.BlockMeta;

import java.util.Map.Entry;

/**
 * @author huang_jie
 *         2/9/2015 4:42 PM
 */
public final class TableIterator extends AbstractSeekingIterator<byte[], byte[]> {
    private final Table.Reader table;
    private final BlockIterator dataBlockIndexIterator;
    private BlockIterator current;

    public TableIterator(Table.Reader table, BlockIterator dataBlockIndexIterator) {
        this.table = table;
        this.dataBlockIndexIterator = dataBlockIndexIterator;
    }

    @Override
    protected void seekToFirstInternal() {
        // reset index to before first and clear the data iterator
        dataBlockIndexIterator.seekToFirst();
        current = null;
    }

    @Override
    protected void seekInternal(byte[] targetKey) {
        // seek the index to the block containing the key
        dataBlockIndexIterator.seek(targetKey);
        //if index iterator does not have a next, it mean the key does not exist in this iterator
        if (dataBlockIndexIterator.hasNext()) {
            //seek the current iterator to the key
            current = getNextBlock();
            current.seek(targetKey);
        } else {
            current = null;
        }
    }

    @Override
    protected Entry<byte[], byte[]> getNextEntry() {
        boolean currentHasNext = false;
        while (true) {
            if (current != null) {
                currentHasNext = current.hasNext();
            }
            if (!currentHasNext) {
                if (dataBlockIndexIterator.hasNext()) {
                    current = getNextBlock();
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        if (currentHasNext) {
            return current.next();
        } else {
            //set current to empty iterator to avoid extra calls to user iterators
            current = null;
            return null;
        }
    }

    private BlockIterator getNextBlock() {
        byte[] blockMetaRaw = dataBlockIndexIterator.next().getValue();
        BlockMeta blockMeta = BlockMeta.readBlockMeta(blockMetaRaw);
        Reader dataBlock = table.openBlock(blockMeta);
        return dataBlock.iterator();
    }
}
