package com.huoteng.placeAnalyzer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by teng on 3/09/16.
 * 周末用户点统计
 */
public class MRCountWeekend {

//  样例数据：04badc199b1cac0d2dafd1613212be3c|2015-04-07 12:18:00.541373|52|460012940818213|115.839400|28.654400|1

    public static class MapCountOneDayPoints extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        private Text keyText = new Text();
        private Text resultText = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String lineData = value.toString();

            String[] userTrack = lineData.split("\\|");

            if (UserStatus.judgeTimeValid(userTrack[1])) {
                //key = MSID|date
                StringBuffer keyStringBuffer = new StringBuffer(userTrack[0]);
                keyStringBuffer.append("|");


                //判断是否周末
                String userDate = new String(userTrack[1].substring(0, 10));
                if(UserStatus.isWeekend(userDate)) {
                    int userTime = UserStatus.getUserTime(userTrack[1]);

                    keyStringBuffer.append(userDate);

                    //trackValue = time,longitude,latitude
                    String trackValue = userTime + "," + userTrack[4] + "," + userTrack[5];

                    keyText.set(keyStringBuffer.toString());
                    resultText.set(trackValue);
                    output.collect(keyText, resultText);
                }
            }
        }
    }


    //统计当天所有点
    public static class ReduceCountOneDayPoints extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        private Text resultText = new Text();

        /**
         * value样例：time,longitude,latitude|time,longitude,latitude|time,longitude,latitude
         */
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            String result = "";
            //字符串拼接,内存应该不会炸
            while (values.hasNext()) {
                String records = values.next().toString();

                result += records;
                result += "|";
            }

            result = result.substring(0, result.length()-2);
            resultText.set(result);
            output.collect(key, resultText);

        }


    }
}
