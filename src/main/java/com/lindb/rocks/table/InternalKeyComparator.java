package com.lindb.rocks.table;

import com.google.common.primitives.Longs;

import java.util.Comparator;

/**
 * @author huang_jie
 *         2/9/2015 1:00 PM
 */
public class InternalKeyComparator implements Comparator<InternalKey> {
    private final UserComparator userComparator;

    public InternalKeyComparator(UserComparator userComparator) {
        this.userComparator = userComparator;
    }

    public UserComparator getUserComparator() {
        return userComparator;
    }

    public String name() {
        return userComparator.name();
    }

    @Override
    public int compare(InternalKey left, InternalKey right) {
        int result = userComparator.compare(left.getUserKey(), right.getUserKey());
        if (result != 0) {
            return result;
        }
        return Longs.compare(right.getTimestamp(), left.getTimestamp()); // reverse sorted version numbers
    }
}
