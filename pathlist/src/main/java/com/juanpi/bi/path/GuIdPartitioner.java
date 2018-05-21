package com.imeijia.bi.path;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partition主要作用就是将map的结果发送到相应的reduce。这就对partition有两个要求：
 * 1. 均衡负载，尽量的将工作均匀的分配给不同的reduce。
 * 2. 效率，分配速度一定要快。
 *
 * Map的结果，会通过partition分发到Reducer上，Reducer 做完reduce操作后，通过OutputFormat，进行输出，下面我们就来分析参与这个过程的类。
 * Mapper的结果，可能送到Combiner做合并，Combiner在系统中并没有自己的基类，而是用Reducer作为Combiner的基类，他们对外的功能是一样的，只是使用的位置和使用时的上下文不太一样而已。
 * Mapper最终处理的键值对<key, value>，是需要送到Reducer去合并的，合并的时候，有相同key的键/值对会送到同一个Reducer那。
 * 哪个key到哪个Reducer的分配过程，是由Partitioner规定的。
 * 输入是Map的结果对<key, value>和Reducer的数目，输出则是分配的Reducer（整数编号）。
 * 就是指定 Mapper 输出的键值对到哪一个reducer上去。系统缺省的Partitioner是HashPartitioner，它以key的Hash值对Reducer的数目取模，得到对应的Reducer。
 * 这样保证如果有相同的key值，肯定被分配到同一个reducre上。如果有N个reducer，编号就为0,1,2,3……(N-1)。
 * 按照partition分区后，每个分区的数据乱序的！！后续会调用{@link PathGroupingComparator}
 * Created by vkzhang@yeah.net on 2017/7/13.
 */
public class GuIdPartitioner extends Partitioner<GuIdDatePair, TextArrayWritable> {
    /**
     * 注意：这里采用默认的hash分区实现方法
     * 根据组合键的第一个值作为分区
     * 这里需要说明一下，如果不自定义分区的话，mapreduce框架会根据默认的hash分区方法，
     * 将整个组合将相等的分到一个分区中，这样的话显然不是我们要的效果
     *
     * @param pair
     * @param text
     * @param numberOfPartitions
     * @return
     */
    @Override
    public int getPartition(GuIdDatePair pair, TextArrayWritable text, int numberOfPartitions) {
        return Math.abs(pair.guId.hashCode() % numberOfPartitions);
    }
}
