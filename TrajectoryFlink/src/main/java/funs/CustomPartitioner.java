package funs;

import org.apache.flink.api.common.functions.Partitioner;

public class CustomPartitioner implements Partitioner<Integer> {
    @Override
    public int partition(Integer key, int numPartitions) {
        return key % numPartitions;
    }
}