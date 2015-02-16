package com.lindb.rocks.table;

import com.google.common.base.Preconditions;

/**
 * @author huang_jie
 *         2/9/2015 1:43 PM
 */
public class InternalUserComparator implements UserComparator {
    private final InternalKeyComparator internalKeyComparator;

    public InternalUserComparator(InternalKeyComparator internalKeyComparator) {
        this.internalKeyComparator = internalKeyComparator;
    }

    @Override
    public String name() {
        return internalKeyComparator.name();
    }

    @Override
    public byte[] findShortestSeparator(byte[] start, byte[] end) {
        //TODO need do this create internal key?
        //attempt to shorten the user portion of the key
        byte[] startUserKey = new InternalKey(start).getUserKey();
        byte[] limitUserKey = new InternalKey(end).getUserKey();
        byte[] shortestSeparator = internalKeyComparator.getUserComparator().findShortestSeparator(startUserKey, limitUserKey);
        if (internalKeyComparator.getUserComparator().compare(startUserKey, shortestSeparator) < 0) {
            // User key has become larger.  Tack on the earliest possible
            // number to the shortened user key.
            InternalKey newInternalKey = new InternalKey(shortestSeparator, SequenceNumber.MAX_SEQUENCE_NUMBER, ValueType.VALUE);
            Preconditions.checkState(compare(start, newInternalKey.encode()) < 0); // todo
            Preconditions.checkState(compare(newInternalKey.encode(), end) < 0); // todo

            return newInternalKey.encode();
        }
        return start;
    }

    @Override
    public byte[] findShortSuccessor(byte[] key) {
        byte[] userKey = new InternalKey(key).getUserKey();
        byte[] shortSuccessor = internalKeyComparator.getUserComparator().findShortSuccessor(userKey);

        if (internalKeyComparator.getUserComparator().compare(userKey, shortSuccessor) < 0) {
            // User key has become larger.  Tack on the earliest possible
            // number to the shortened user key.
            InternalKey newInternalKey = new InternalKey(shortSuccessor, SequenceNumber.MAX_SEQUENCE_NUMBER, ValueType.VALUE);
            Preconditions.checkState(compare(key, newInternalKey.encode()) < 0); // todo

            return newInternalKey.encode();
        }

        return key;
    }

    @Override
    public int compare(byte[] left, byte[] right) {
        return internalKeyComparator.compare(new InternalKey(left), new InternalKey(right));
    }
}
