package com.lindb.rocks;

import java.util.Comparator;

/**
 * @author huang_jie
 *         2/10/2015 3:07 PM
 */
public interface DBComparator extends Comparator<byte[]> {
    String name();

    /**
     * If <code>start < limit</code>, returns a short key in [start,limit).
     * Simple comparator implementations should return start unchanged,
     */
    byte[] findShortestSeparator(byte[] start, byte[] limit);

    /**
     * returns a 'short key' where the 'short key' >= key.
     * Simple comparator implementations should return key unchanged,
     */
    byte[] findShortSuccessor(byte[] key);
}
