package com.huoteng.residentSumCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Created by bixia on 2015/12/16.
 */
public class MRResidentTotal {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        private Text keyText = new Text();
        private Text resultText = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String lineData = value.toString();

            String[] userTrack = lineData.split("\\t");

            if(userTrack.length >= 2) {
                String keyDateString = userTrack[0];

                String[] userMSIDDate = keyDateString.split("\\|");

                keyText.set(userMSIDDate[0]);
                resultText.set(userMSIDDate[1]);
                output.collect(keyText, resultText);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        private Text resultText = new Text();

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            List<String> dateList = new ArrayList<String>();

            while(values.hasNext()) {
                String date = values.next().toString();
                int index = 0;
                for(; index < dateList.size(); index++) {
                    if(date.equals(dateList.get(index))) {
                        break;
                    }
                }
                if(index == dateList.size()) {
                    dateList.add(date);
                }
            }

            if(dateList.size() >= 6) {
                resultText.set("1");
                output.collect(key, resultText);
            }
        }
    }
}
