package com.imeijia.bi.path;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;

import static org.apache.hadoop.io.WritableComparator.readVLong;

/**
 * Created by vkzhang@yeah.net on 2017/7/13.
 */
public class RawGroupingComparator implements RawComparator<GuIdDatePair> {

    @Override
    public int compare(GuIdDatePair o1, GuIdDatePair o2) {
        // lexicographically compareTo按照字典序排序
        return o1.getGuId().compareTo(o2.getGuId());
    }

    /*
    * 其中b1为第一个对象所在的字节数组，s1为该对象在b1中的起始位置，l1为对象在b1中的长度，
    * b2为第一个对象所在的字节数组，s2为该对象在b2中的起始位置，l2为对象在b2中的长度。
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

        int cmp;
        //determine how many bytes the guId VLong takes
        int n1 = WritableUtils.decodeVIntSize(b1[s1]);
        int n2 = WritableUtils.decodeVIntSize(b2[s2]);

        try {
            long l11 = readVLong(b1, s1);
            long l21 = readVLong(b2, s2);

            cmp = l11 > l21 ? 1 : (l11 == l21 ? 0 : -1);
            if (cmp != 0) {

                return cmp;

            } else {

                long l12 = readVLong(b1, s1 + n1);
                long l22 = readVLong(b2, s2 + n2);
                return l12 > l22 ? 1 : (l12 == l22 ? 0 : -1);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}