package com.huoteng.statisticsCount;

import com.huoteng.placeAnalyzer.UserStatus;
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
public class MRCountResultEachWorkHomePlaceNum {
    public static class CountMap extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text keyText = new Text();
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineData = value.toString();

            String[] userTrack = lineData.split("\t");

            if(userTrack.length >= 2) {
                String keyPlaceString = userTrack[0];
                String[] userMSIDPlace = keyPlaceString.split("\\|");
                int userPlace = Integer.parseInt(userMSIDPlace[1]);
                if(userPlace == UserStatus.HOME) {
                    keyText.set("home");
                } else if(userPlace == UserStatus.WORK) {
                    keyText.set("work");
                }
                context.write(keyText, one);
            }
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

        Job jobCount = new Job(new Configuration(), MRCountResultEachWorkHomePlaceNum.class.getName());
        jobCount.setJarByClass(MRCountResultEachWorkHomePlaceNum.class);

        jobCount.setMapperClass(MRCountResultEachWorkHomePlaceNum.CountMap.class);
        jobCount.setReducerClass(MRCountResultEachWorkHomePlaceNum.CountReduce.class);

        FileInputFormat.setInputPaths(jobCount, new Path("result_workHomePlace"));

        Path outputPath = new Path("result_resultEachWorkHomePlaceNum");
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(jobCount, outputPath);

        jobCount.setOutputKeyClass(Text.class);
        jobCount.setOutputValueClass(IntWritable.class);

        jobCount.waitForCompletion(true);
    }
}
