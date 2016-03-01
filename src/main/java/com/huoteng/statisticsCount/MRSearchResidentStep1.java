package com.huoteng.statisticsCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by bixia on 2015/12/22.
 */
public class MRSearchResidentStep1 {

    private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static DateFormat weekFormat = new SimpleDateFormat("E");

    public static class SearchMap extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text keyText = new Text();
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineData = value.toString();

            String[] userTrack = lineData.split("\\|");

            if(userTrack.length >= 2) {
                String keyString = userTrack[0];
                String userDateString = new String(userTrack[1].substring(0, 10));

                try {
                    Date userDate = dateFormat.parse(userDateString);
                    String week = weekFormat.format(userDate);

                    if(!week.equals("Sun") && !week.equals("Sat")) {
                        if(!userDateString.equals("2015-04-07")) {
                            keyText.set(keyString + "|" + userDateString);
                            context.write(keyText, one);
                        }
                    }

                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
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
