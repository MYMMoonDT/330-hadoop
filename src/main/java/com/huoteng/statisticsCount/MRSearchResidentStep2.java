package com.huoteng.statisticsCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by bixia on 2015/12/22.
 */
public class MRSearchResidentStep2 {
    public static class SearchMap extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text keyText = new Text();
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineData = value.toString();

            String[] userTrack = lineData.split("\t");

            if(userTrack.length == 2) {
                String keyDateString = userTrack[0];
                String[] userMSIDDate = keyDateString.split("\\|");
                if(userMSIDDate.length == 2) {
                    keyText.set(userMSIDDate[0]);
                    context.write(keyText, one);
                }
            }
        }
    }

    public static class SearchReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for(IntWritable value : values) {
                sum += value.get();
            }

            if(sum >= 6) {
                context.write(key, one);
            }
        }
    }
}
