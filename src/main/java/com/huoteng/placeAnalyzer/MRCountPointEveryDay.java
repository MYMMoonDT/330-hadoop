package com.huoteng.placeAnalyzer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Created by teng on 12/7/15.
 */
public class MRCountPointEveryDay {

    public static class TrackMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        private Text keyText = new Text();
        private Text resultText = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String lineData = value.toString();

            String[] userTrack = lineData.split("\\|");

            if (UserStatus.judgeTimeValid(userTrack[1])) {
                //key = MSID|date
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
                }

                keyStringBuffer.append(userDate);
                keyStringBuffer.append("|");
                keyStringBuffer.append(userPlace);

                //trackValue = time,longitude,latitude
                String trackValue = userTime + "," + userTrack[4] + "," + userTrack[5];

                keyText.set(keyStringBuffer.toString());
                resultText.set(trackValue);
                output.collect(keyText, resultText);
            }
        }
    }



    public static class TrackCombine extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        private Text resultText = new Text();

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            List<Coordinate> coordinatesList = new ArrayList<Coordinate>();

            while (values.hasNext()) {
                String records = values.next().toString();

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
            if (tmp[2].equals(Integer.toString(UserStatus.HOME))) {
                String homePointsString = UserStatus.getHomeTimePoint(coordinatesList, false);
                resultText.set(homePointsString);
            } else if (tmp[2].equals(Integer.toString(UserStatus.WORK))) {
                String worksPointString = UserStatus.getWorkTimePoint(coordinatesList, false);
                resultText.set(worksPointString);
            }
            output.collect(key, resultText);
        }
    }


    public static class TrackReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        private Text resultText = new Text();

        /**
         * value样例：time,longitude,latitude|time,longitude,latitude|time,longitude,latitude
         */
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            List<Coordinate> coordinatesList = new ArrayList<Coordinate>();

            while (values.hasNext()) {
                String records = values.next().toString();

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
            if (tmp[2].equals(Integer.toString(UserStatus.HOME))) {
                String homePointsString = UserStatus.getHomeTimePoint(coordinatesList, true);
                resultText.set(homePointsString);
            } else if (tmp[2].equals(Integer.toString(UserStatus.WORK))) {
                String worksPointString = UserStatus.getWorkTimePoint(coordinatesList, true);
                resultText.set(worksPointString);
            }
            output.collect(key, resultText);

        }


    }
}
