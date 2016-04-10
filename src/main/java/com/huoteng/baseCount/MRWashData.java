package com.huoteng.baseCount;

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
 * Created by teng on 12/7/15.
 */
public class MRWashData {
    private static DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static DateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");

    /**
     * 得到整点时间,每个整点的前70分钟记为该整点
     * @param userDateTime 2015-04-07 11:12:00
     * @return RoundTime
     */
    private static RoundTime getSharpTime(String userDateTime) {

        try {
            Date currentDateTime = dateTimeFormat.parse(userDateTime);
            currentDateTime.setTime(currentDateTime.getTime() + (70 * 60 * 1000));

            String dateTimeStr = dateTimeFormat.format(currentDateTime);
            String roundTimeStr = new String(dateTimeStr.substring(0, 14) + "00:00");

            Date roundTime = dateTimeFormat.parse(roundTimeStr);
            long distance = roundTime.getTime() - currentDateTime.getTime();

            RoundTime time = new RoundTime(roundTimeStr, distance);

            return  time;
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 判断时间是否需要,这次只需要工作日的数据,暂时用不着该函数
     * @param userDateTime 2015-04-07 12:18:00
     * @return boolean
     */
//    private static boolean judgeTimeIsValued(String userDateTime) {
//        try {
//            Date date = dateTimeFormat.parse(userDateTime);
//            String day = weekFormat.format(date);
//
//            if (day.equals("Sat") || day.equals("Sun")) {
//                return false;
//            } else {
//                return true;
//            }
//        } catch (ParseException e) {
//            e.printStackTrace();
//            return false;
//        }
//    }


    /**
     * 判断事件类型:12,13,14,15为通话事件,其他为综合事件
     * @param actionNum 事件代码
     * @return 事件类型,call/other
     */
    private static String judgeActionType(int actionNum) {

        switch (actionNum) {
            case 12:
            case 13:
            case 14:
            case 15:
                return "call";
            default:
                return "other";
        }
    }

    /**
     * 比较两个时间哪个更靠近整点,秒数越小离整点越近
     * @param firstPointDistance firstPointDistance
     * @param secondPointDistance secondPointDistance
     * @return
     */
    private static boolean newPointIsClose(long firstPointDistance, long secondPointDistance) {

        if (firstPointDistance - secondPointDistance < 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Map1:
     * From:    MSID|时间|事件|基站编号|经度|纬度|不知道什么鬼
     * To:      MSID|整点日期时间    基站编号|经纬度|距离整点秒数|事件类型  old data format:MSID|整点日期时间    基站编号|经纬度|精确时间
     */
    public static class WashDataMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        private Text keyText = new Text();
        private Text resultText = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String lineData = value.toString();

            String[] rawData = lineData.split("\\|");
            String dateTime = new String(rawData[1].substring(0, 19));

            //每个整点的前70分钟记为该整点
            //判断事件类型:12,13,14,15为通话事件,其他为综合事件
            RoundTime time = getSharpTime(dateTime);
            String msid = rawData[0];
            String signalStationNum = rawData[3];
            String coordianteString = rawData[4] + "," + rawData[5];

            int actionNum = Integer.parseInt(rawData[2]);
            String actionType = judgeActionType(actionNum);

            keyText.set(msid + "|" + time.roundTimeString);
            resultText.set(signalStationNum + "|" + coordianteString + "|" + time.roundTimeDistance + "|" + actionType);
            output.collect(keyText, resultText);
        }
    }

    /**
     * Reduce1:
     * 保留一条最靠近整点的数据
     * MSID|整点日期时间    基站编号|经纬度|距离整点秒数|事件类型
     */
    public static class WashDataReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        private Text resultText = new Text();

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            MostClosedSharpTimePoint bestPoint = new MostClosedSharpTimePoint();

            while (values.hasNext()) {
                String records = values.next().toString();

                String[] baseInfo = records.split("\\|");

                long newPoint = Long.parseLong(baseInfo[2]);
                //判断距离整点秒数,取离整点时间近的点

                //比较两个点哪个更靠近整点
                if (null == bestPoint.signalStationNum || newPointIsClose(bestPoint.distance, newPoint)) {
                    bestPoint.signalStationNum = baseInfo[0];
                    bestPoint.coordinateStr = baseInfo[1];
                    bestPoint.distance = newPoint;
                    bestPoint.actionType = baseInfo[3];
                }
            }

            resultText.set(bestPoint.signalStationNum + "|" + bestPoint.coordinateStr + "|" + bestPoint.distance + "|" + bestPoint.actionType);
            output.collect(key, resultText);
        }
    }
}

class RoundTime{
    public String roundTimeString;
    public long roundTimeDistance;

    RoundTime(String timeStr, long distance) {
        roundTimeString = timeStr;
        roundTimeDistance = distance;
    }
}

class MostClosedSharpTimePoint {
    public String signalStationNum;
    public String coordinateStr;
    public long distance;
    public String actionType;
}