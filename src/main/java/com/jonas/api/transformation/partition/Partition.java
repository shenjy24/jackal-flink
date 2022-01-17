package com.jonas.api.transformation.partition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * 自定义分区规则
 */
public class Partition implements Partitioner<Long> {
    @Override
    public int partition(Long key, int numPartitions) {
        System.out.println("分区总数：" + numPartitions);
        if (key % 2 == 0) {
            return 0;
        } else {
            return 1;
        }
    }
}
