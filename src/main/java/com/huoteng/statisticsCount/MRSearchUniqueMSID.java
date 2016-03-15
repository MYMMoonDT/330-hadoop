package com.huoteng.statisticsCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by bixia on 2015/12/21.
 */
public class MRSearchUniqueMSID {
    public static class SearchMap extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text keyText = new Text();
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineData = value.toString();

            String[] userTrack = lineData.split("\\|");

            keyText.set(userTrack[0]);

            context.write(keyText, one);
        }
    }

    public static class SearchReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, one);
        }
    }
}
