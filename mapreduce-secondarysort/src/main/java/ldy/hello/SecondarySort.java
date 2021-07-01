package ldy.hello;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SecondarySort {

    public static class SecondarySortMapper extends Mapper<LongWritable, Text, PairWritable, IntWritable> {

        private PairWritable mapOutputKey = new PairWritable();
        private IntWritable mapOutputValue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, PairWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] strs = line.split(" ");
            mapOutputKey.set(Integer.valueOf(strs[0]), Integer.valueOf(strs[1]));
            mapOutputValue.set(Integer.valueOf(strs[1]));
            context.write(mapOutputKey, mapOutputValue);
        }
    }

    public static class SecondarySortReducer extends Reducer<PairWritable, IntWritable, IntWritable, IntWritable> {

        private IntWritable outputKey = new IntWritable();
        
        @Override
        protected void reduce(PairWritable key, Iterable<IntWritable> values,
                Reducer<PairWritable, IntWritable, IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {
            for (IntWritable value : values) {
                outputKey.set(key.getFirst());
                context.write(outputKey, value);
            }
        }
    }

    public static void main(String[] args) 
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "secondarysort");
        job.setJarByClass(SecondarySort.class);
        
        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(SecondarySortMapper.class);
        
        // shuffle settings
        job.setPartitionerClass(FirstPartitioner.class);
        job.setGroupingComparatorClass(FirstGroupingComparator.class);
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(SecondarySortReducer.class);
        
        FileInputFormat.addInputPath(job,   new Path("/datas/secondarysort-input"));
        FileOutputFormat.setOutputPath(job, new Path("/datas/secondarysort-output"));
        
        boolean isSuccess = job.waitForCompletion(true);
        System.exit(isSuccess ? 0 : 1);
    }
}

