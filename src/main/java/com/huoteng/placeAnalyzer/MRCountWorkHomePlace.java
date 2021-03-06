package com.huoteng.placeAnalyzer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 对每天的点统计，找出最终的工作地和居住地
 * 第一次mapreduce的数据和丁亮的数据完全吻合，所以推断应该是第二次mapreduce有问题，初步判断是map的问题，因为16号的工作地匹配结果就与丁亮的数据相差较多
 * 具体问题还需要进一步的数据比对
 *
 * Created by teng on 12/7/15.
 */
public class MRCountWorkHomePlace {

    /**
     * 统计一天的居住地和工作地
     * 16号的数据与丁亮的数据完全吻合（12.13）
     */
    public static class WorkHomePlaceMap extends Mapper<LongWritable, Text, Text, Text> {

        private Text keyText = new Text();
        private Text resultText = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineData = value.toString();

            String[] userTrack = lineData.split("\t");

            if (userTrack.length >= 2) {
                String keyDatePlaceString = userTrack[0];
                String track = userTrack[1];

                //000015cac9cb2c30cb32de1dd5e149b3|2015-04-07|5
                String[] userMSIDDatePlace = keyDatePlaceString.split("\\|");

                if(!userMSIDDatePlace[1].equals("2015-04-07")) {

                    String[] tracksArray = track.split("\\|");

                    List<Coordinate> oneDayCoordinates = new ArrayList<Coordinate>();

                    for (String point : tracksArray) {
                        if (!point.equals("")) {
                            String[] pointDetail = point.split(",");

                            int time = Integer.parseInt(pointDetail[0]);
                            oneDayCoordinates.add(new Coordinate(pointDetail[2], pointDetail[1], time));
                        }
                    }

                    String mapResult = ReduceStatistics.countPoint(oneDayCoordinates, 3);
                    if (!mapResult.equals("")) {

                        keyText.set(userMSIDDatePlace[0] + "|" + userMSIDDatePlace[2]);
                        resultText.set(mapResult);
                        context.write(keyText, resultText);

                    }

                }
            } else {
                System.out.println("Empty LINE:" + lineData);
            }

        }
    }


    /**
     * 统计10天的工作地和居住地
     */
    public static class WorkHomePlaceReduce extends Reducer<Text, Text, Text, Text> {

        private Text resultText = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<Coordinate> coordinatesList = new ArrayList<Coordinate>();

            for (Text value : values) {
                String oneDayRecord = value.toString();

                String[] recordDetail = oneDayRecord.split(",");
                int time = Integer.parseInt(recordDetail[0]);
                coordinatesList.add(new Coordinate(recordDetail[2], recordDetail[1], time));
            }

            //10天的数据取有6天以上认为有效
            String result = ReduceStatistics.countPoint(coordinatesList, 6);
            // 正常
            if (!result.equals("")) {
                resultText.set(result);
                context.write(key, resultText);
            }
        }
    }

}
