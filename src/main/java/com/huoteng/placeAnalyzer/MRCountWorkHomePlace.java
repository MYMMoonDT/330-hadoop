package com.huoteng.placeAnalyzer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by teng on 12/7/15.
 */
public class MRCountWorkHomePlace {

    public static class WorkHomePlaceMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        private Text keyText = new Text();
        private Text resultText = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String lineData = value.toString();

            String[] userTrack = lineData.split("\\t");

            if (userTrack.length >= 2) {
                String keyTimePlaceString = userTrack[0];
                String track = userTrack[1];

                String[] tracksArray = track.split("\\|");

                List<Coordinate> oneDayCoordinates = new ArrayList<Coordinate>();

                for (String onePoint : tracksArray) {
                    if (!onePoint.equals("")) {
                        String[] pointDetail = onePoint.split(",");

                        int time = Integer.parseInt(pointDetail[0]);
                        oneDayCoordinates.add(new Coordinate(pointDetail[2], pointDetail[1], time));
                    }
                }

                String reduceResult = ReduceStatistics.countPoint(oneDayCoordinates, 3);
                if (!reduceResult.equals("")) {
                    //000015cac9cb2c30cb32de1dd5e149b3|2015-04-07|5
                    String[] userMSIDTimePlace = keyTimePlaceString.split("\\|");

                    keyText.set(userMSIDTimePlace[0] + "|" +  userMSIDTimePlace[2]);
                    resultText.set(reduceResult);
                    output.collect(keyText, resultText);
                }
            } else {
                System.out.println("LINE:" + lineData);
            }

        }
    }


    public static class WorkHomePlaceReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        private Text resultText = new Text();

        /**
         * value样例：time,longitude,latitude|time,longitude,latitude|time,longitude,latitude
         */
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            List<Coordinate> coordinatesList = new ArrayList<Coordinate>();

            while (values.hasNext()) {
                String oneDayRecord = values.next().toString();

                String[] recordDetail = oneDayRecord.split(",");
                int time = Integer.parseInt(recordDetail[0]);
                coordinatesList.add(new Coordinate(recordDetail[2], recordDetail[1], time));
            }

            //10天的数据取有6天以上认为有效
            String result = ReduceStatistics.countPoint(coordinatesList, 6);
            if (!result.equals("")) {
                resultText.set(result);
                output.collect(key, resultText);
            }
        }


    }
}
