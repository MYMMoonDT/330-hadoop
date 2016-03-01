package com.huoteng.placeAnalyzer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * 第一个Mapreduce没有问题，问题出在第二个Map Reduce上
 * Map Reduce获得每天每个用户在家的和在工作地的五个点
 * Created by teng on 12/7/15.
 */
public class MRCountPointEveryDay {

    //Map处理原始数据，将每个用户的的时间处理为当天从0点开始的秒数
    public static class TrackMap extends Mapper<LongWritable, Text, Text, Text> {

        private Text keyText = new Text();
        private Text resultText = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineData = value.toString();

            String[] userTrack = lineData.split("\\|");

            if (UserStatus.judgeTimeValid(userTrack[1])) {
                //key = MSID|date|place
                StringBuffer keyStringBuffer = new StringBuffer(userTrack[0]);
                keyStringBuffer.append("|");

                String userDate = new String(userTrack[1].substring(0, 10));
                int userPlace = Integer.parseInt(UserStatus.judgeUserPlace(userTrack[1]));
                int userTime = UserStatus.getUserTime(userTrack[1]);
                if (UserStatus.HOME == userPlace && userTime >= UserStatus.TIME_20_00) {
                    //在提取休息地点的时候需要将20到24点的点算在前一天内
                    int day = Integer.parseInt(userDate.substring(8, 10));
                    day++;
                    userDate = userDate.substring(0, 8) + new DecimalFormat("00").format(day);

                    //并且将time换算成负数
                    userTime = userTime - UserStatus.TIME_24_00;
                }

                keyStringBuffer.append(userDate);
                keyStringBuffer.append("|");
                keyStringBuffer.append(userPlace);

                //trackValue = time,longitude,latitude
                String trackValue = userTime + "," + userTrack[4] + "," + userTrack[5];

                keyText.set(keyStringBuffer.toString());
                resultText.set(trackValue);
                context.write(keyText, resultText);
            }
        }
    }


    //统计每天的五个点
    public static class TrackReduce extends Reducer<Text, Text, Text, Text> {

        private Text resultText = new Text();

        /**
         * value样例：time,longitude,latitude|time,longitude,latitude|time,longitude,latitude
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<Coordinate> coordinatesList = new ArrayList<Coordinate>();

            for (Text value : values) {
                String records = value.toString();

                String[] recordsArray = records.split("\\|");
                for (String oneRecord : recordsArray) {
                    if (!oneRecord.equals("")) {
                        String[] oneRecordArray = oneRecord.split(",");

                        int userTime = Integer.parseInt(oneRecordArray[0]);
                        coordinatesList.add(new Coordinate(oneRecordArray[2], oneRecordArray[1], userTime));
                    }
                }
            }

            Collections.sort(coordinatesList);

            String keyString = key.toString();
            String[] tmp = keyString.split("\\|");
            String pointsStr = UserStatus.getFivePoints(coordinatesList, Integer.parseInt(tmp[2]));

            resultText.set(pointsStr);
            context.write(key, resultText);

        }


    }
}
