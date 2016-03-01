package com.huoteng.statisticsCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by bixia on 2015/12/21.
 */
public class MRCountUniqueMSIDNum {
    public static class CountMap extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text keyText = new Text();
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            keyText.set("total");

            context.write(keyText, one);
        }
    }

    public static class CountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

         @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job jobSearch = new Job(new Configuration(), MRSearchUniqueMSID.class.getName());
        jobSearch.setJarByClass(MRSearchUniqueMSID.class);

        jobSearch.setMapperClass(MRSearchUniqueMSID.SearchMap.class);
        jobSearch.setReducerClass(MRSearchUniqueMSID.SearchReduce.class);

        FileInputFormat.setInputPaths(jobSearch, new Path("big_input"));

        Path outputPath = new Path("middle_uniqueMSIDNum");
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(jobSearch, outputPath);

        jobSearch.setOutputKeyClass(Text.class);
        jobSearch.setOutputValueClass(IntWritable.class);

        jobSearch.waitForCompletion(true);

        Job jobCount = new Job(new Configuration(), MRCountUniqueMSIDNum.class.getName());
        jobCount.setJarByClass(MRCountUniqueMSIDNum.class);

        jobCount.setMapperClass(MRCountUniqueMSIDNum.CountMap.class);
        jobCount.setReducerClass(MRCountUniqueMSIDNum.CountReduce.class);

        FileInputFormat.setInputPaths(jobCount, new Path("middle_uniqueMSIDNum"));

        outputPath = new Path("result_uniqueMSIDNum");
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(jobCount, outputPath);

        jobCount.setOutputKeyClass(Text.class);
        jobCount.setOutputValueClass(IntWritable.class);

        jobCount.waitForCompletion(true);
    }
}
