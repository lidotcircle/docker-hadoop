package ldy.hello;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;


public class FirstPartitioner extends Partitioner<PairWritable, IntWritable> {

    @Override
    public int getPartition(PairWritable key, IntWritable value, 
            int numPartitions) {
        return (String.valueOf(key.getFirst()).hashCode() & Integer.MAX_VALUE) % numPartitions;
    }

}

