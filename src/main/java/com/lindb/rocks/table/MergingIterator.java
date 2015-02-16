package com.lindb.rocks.table;

import com.google.common.primitives.Ints;

import java.util.*;
import java.util.Map.Entry;

public final class MergingIterator extends AbstractSeekingIterator<InternalKey, byte[]> {
    private final List<? extends InternalIterator> levels;
    private final PriorityQueue<ComparableIterator> priorityQueue;
    private final Comparator<InternalKey> comparator;

    public MergingIterator(List<? extends InternalIterator> levels, Comparator<InternalKey> comparator) {
        this.levels = levels;
        this.comparator = comparator;

        this.priorityQueue = new PriorityQueue<>(levels.size() + 1);
        resetPriorityQueue(comparator);
    }

    @Override
    protected void seekToFirstInternal() {
        for (InternalIterator level : levels) {
            level.seekToFirst();
        }
        resetPriorityQueue(comparator);
    }

    @Override
    protected void seekInternal(InternalKey targetKey) {
        for (InternalIterator level : levels) {
            level.seek(targetKey);
        }
        resetPriorityQueue(comparator);
    }

    private void resetPriorityQueue(Comparator<InternalKey> comparator) {
        int i = 1;
        for (InternalIterator level : levels) {
            if (level.hasNext()) {
                priorityQueue.add(new ComparableIterator(level, comparator, i++, level.next()));
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
        private final InternalIterator iterator;
        private final Comparator<InternalKey> comparator;
        private final int ordinal;
        private Entry<InternalKey, byte[]> nextElement;

        private ComparableIterator(InternalIterator iterator, Comparator<InternalKey> comparator, int ordinal, Entry<InternalKey, byte[]> nextElement) {
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
