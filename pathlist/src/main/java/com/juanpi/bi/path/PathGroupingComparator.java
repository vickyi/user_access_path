package com.imeijia.bi.path;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 控制相同的key输出到同一个reduce
 * Created by vkzhang@yeah.net on 2017/7/13.
 */
public class PathGroupingComparator extends WritableComparator {

    public PathGroupingComparator() {
            super(GuIdDatePair.class, true);
        }

        @Override
        /**
         * Compare two objects
         *
         * @param wc1 a WritableComparable object, which represents a GuIdDatePair
         * @param wc2 a WritableComparable object, which represents a GuIdDatePair
         * @return 0, 1, or -1 (depending on the comparison of two GuIdDatePair objects).
         */
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            GuIdDatePair pair = (GuIdDatePair) wc1;
            GuIdDatePair pair2 = (GuIdDatePair) wc2;
            return pair.getGuId().compareTo(pair2.getGuId());
        }
    }
