package com.lindb.rocks.table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;

import java.util.*;
import java.util.Map.Entry;

public final class Level0Iterator extends AbstractSeekingIterator<InternalKey, byte[]> implements InternalIterator {
    private final List<InternalTableIterator> inputs;
    private final PriorityQueue<ComparableIterator> priorityQueue;
    private final Comparator<InternalKey> comparator;

    public Level0Iterator(TableCache tableCache, List<FileMetaData> files, Comparator<InternalKey> comparator) {
        Builder<InternalTableIterator> builder = ImmutableList.builder();
        for (FileMetaData file : files) {
            builder.add(tableCache.newIterator(file));
        }
        this.inputs = builder.build();
        this.comparator = comparator;

        this.priorityQueue = new PriorityQueue<ComparableIterator>(Iterables.size(inputs) + 1);
        resetPriorityQueue(comparator);
    }

    public Level0Iterator(List<InternalTableIterator> inputs, Comparator<InternalKey> comparator) {
        this.inputs = inputs;
        this.comparator = comparator;

        this.priorityQueue = new PriorityQueue<ComparableIterator>(Iterables.size(inputs));
        resetPriorityQueue(comparator);
    }

    @Override
    protected void seekToFirstInternal() {
        for (InternalTableIterator input : inputs) {
            input.seekToFirst();
        }
        resetPriorityQueue(comparator);
    }

    @Override
    protected void seekInternal(InternalKey targetKey) {
        for (InternalTableIterator input : inputs) {
            input.seek(targetKey);
        }
        resetPriorityQueue(comparator);
    }

    private void resetPriorityQueue(Comparator<InternalKey> comparator) {
        int i = 0;
        for (InternalTableIterator input : inputs) {
            if (input.hasNext()) {
                priorityQueue.add(new ComparableIterator(input, comparator, i++, input.next()));
            }
        }
    }

    @Override
    protected Entry<InternalKey, byte[]> getNextEntry() {
        Entry<InternalKey, byte[]> result = null;
        ComparableIterator nextIterator = priorityQueue.poll();
        if (nextIterator != null) {
            result = nextIterator.next();
            if (nextIterator.hasNext()) {
                priorityQueue.add(nextIterator);
            }
        }
        return result;
    }

    private static class ComparableIterator implements Iterator<Entry<InternalKey, byte[]>>, Comparable<ComparableIterator> {
        private final SeekingIterator<InternalKey, byte[]> iterator;
        private final Comparator<InternalKey> comparator;
        private final int ordinal;
        private Entry<InternalKey, byte[]> nextElement;

        private ComparableIterator(SeekingIterator<InternalKey, byte[]> iterator, Comparator<InternalKey> comparator, int ordinal, Entry<InternalKey, byte[]> nextElement) {
            this.iterator = iterator;
            this.comparator = comparator;
            this.ordinal = ordinal;
            this.nextElement = nextElement;
        }

        @Override
        public boolean hasNext() {
            return nextElement != null;
        }

        @Override
        public Entry<InternalKey, byte[]> next() {
            if (nextElement == null) {
                throw new NoSuchElementException();
            }

            Entry<InternalKey, byte[]> result = nextElement;
            if (iterator.hasNext()) {
                nextElement = iterator.next();
            } else {
                nextElement = null;
            }
            return result;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int compareTo(ComparableIterator that) {
            int result = comparator.compare(this.nextElement.getKey(), that.nextElement.getKey());
            if (result == 0) {
                result = Ints.compare(this.ordinal, that.ordinal);
            }
            return result;
        }
    }
}
