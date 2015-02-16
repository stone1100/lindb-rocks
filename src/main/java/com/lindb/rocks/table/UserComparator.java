package com.lindb.rocks.table;

import java.util.Comparator;

/**
 * @author huang_jie
 *         2/9/2015 12:59 PM
 */
public interface UserComparator extends Comparator<byte[]> {
    String name();

    byte[] findShortestSeparator(byte[] start, byte[] end);

    byte[] findShortSuccessor(byte[] key);
}
