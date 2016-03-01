package com.huoteng.residentSumCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

/**
 * Created by bixia on 2015/12/16.
 */
public class MRResidentEveryDay {
    private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static DateFormat weekFormat = new SimpleDateFormat("E");

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        private Text keyText = new Text();
        private Text resultText = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String lineData = value.toString();

            String[] userTrack = lineData.split("\\|");

            String userDateTimeString = userTrack[1];
            String userDate = new String(userDateTimeString.substring(0, 10));

            try {
                Date userDateTime = dateFormat.parse(userDate);
                String week = weekFormat.format(userDateTime);
                if(!week.equals("Sat") && !week.equals("Sun")) {
                    keyText.set(userTrack[0] + "|" + userDate);
                    resultText.set("1");
                    output.collect(keyText, resultText);
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        private Text resultText = new Text();

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            resultText.set("1");
            output.collect(key, resultText);
        }
    }
}
