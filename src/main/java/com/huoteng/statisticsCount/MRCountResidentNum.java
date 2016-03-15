package com.huoteng.statisticsCount;

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

import java.io.IOException;

/**
 * Created by bixia on 2015/12/21.
 */
public class MRCountResidentNum {
    public static class CountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text keyText = new Text();
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            keyText.set("resident");
            context.write(keyText, one);
        }
    }

    public static class CountReduce extends Reducer<Text, IntWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job jobSearchStep1 = new Job(new Configuration(), MRSearchResidentStep1.class.getName());
        jobSearchStep1.setJarByClass(MRSearchResidentStep1.class);

        jobSearchStep1.setMapperClass(MRSearchResidentStep1.SearchMap.class);
        jobSearchStep1.setReducerClass(MRSearchResidentStep1.SearchReduce.class);

        FileInputFormat.setInputPaths(jobSearchStep1, new Path("big_input"));

        Path outputPath = new Path("middle_residentNumStep1");
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(jobSearchStep1, outputPath);

        jobSearchStep1.setOutputKeyClass(Text.class);
        jobSearchStep1.setOutputValueClass(IntWritable.class);

        jobSearchStep1.waitForCompletion(true);

        Job jobSearchStep2 = new Job(new Configuration(), MRSearchResidentStep2.class.getName());
        jobSearchStep2.setJarByClass(MRSearchResidentStep2.class);

        jobSearchStep2.setMapperClass(MRSearchResidentStep2.SearchMap.class);
        jobSearchStep2.setReducerClass(MRSearchResidentStep2.SearchReduce.class);

        FileInputFormat.setInputPaths(jobSearchStep2, new Path("middle_residentNumStep1"));

        outputPath = new Path("middle_residentNumStep2");
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(jobSearchStep2, outputPath);

        jobSearchStep2.setOutputKeyClass(Text.class);
        jobSearchStep2.setOutputValueClass(IntWritable.class);

        jobSearchStep2.waitForCompletion(true);

        Job jobCount = new Job(new Configuration(), MRCountResidentNum.class.getName());
        jobCount.setJarByClass(MRCountResidentNum.class);

        jobCount.setMapperClass(MRCountResidentNum.CountMap.class);
        jobCount.setReducerClass(MRCountResidentNum.CountReduce.class);

        FileInputFormat.setInputPaths(jobCount, new Path("middle_residentNumStep2"));

        outputPath = new Path("result_residentNum");
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(jobCount, outputPath);

        jobCount.setOutputKeyClass(Text.class);
        jobCount.setOutputValueClass(IntWritable.class);

        jobCount.waitForCompletion(true);
    }
}
