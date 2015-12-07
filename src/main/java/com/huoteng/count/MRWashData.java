package com.huoteng.count;

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
    private static DateFormat weekFormat = new SimpleDateFormat("E");

    /** completed
     * 得到整点时间,考虑为整点过后的15分钟内
     * @param userDateTime 2015-04-07 11:12:00
     * @return 2015-04-07 11:00:00
     */
    private static String getSharpTime(String userDateTime) {
        String tmp = new String(userDateTime.substring(0, 14));
        String beforTime = tmp + "00:00";
        String afterTime = tmp + "15:00";
        try {
            Date beforeDateTime = dateTimeFormat.parse(beforTime);
            Date afterDateTime = dateTimeFormat.parse(afterTime);
            Date currentDateTime = dateTimeFormat.parse(userDateTime);

            if ((currentDateTime.getTime() >= beforeDateTime.getTime()) && (currentDateTime.getTime() <= afterDateTime.getTime())) {
                return beforTime;
            } else {
                return null;
            }
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    /** completed
     * 判断时间是否需要,这次只需要工作日的数据
     * @param userDateTime 2015-04-07 12:18:00
     * @return boolean
     */
    private static boolean judgeTimeIsValued(String userDateTime) {
        try {
            Date date = dateTimeFormat.parse(userDateTime);
            String day = weekFormat.format(date);

            if (day.equals("Sat") || day.equals("Sun")) {
                return false;
            } else {
                return true;
            }
        } catch (ParseException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Map1:
     * MSID|整点日期时间    基站编号|经纬度|精确时间
     */
    public static class WashDataMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        private Text keyText = new Text();
        private Text resultText = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String lineData = value.toString();

            String[] dataDetail = lineData.split("\\|");
            String dateTime = new String(dataDetail[1].substring(0, 19));

            if (judgeTimeIsValued(dateTime)) {
                String sharpTime = getSharpTime(dateTime);

                if (null != sharpTime) {
                    String accurateTime = new String(dataDetail[1].substring(12, 19));
                    String MSID = dataDetail[0];
                    String baseNum = dataDetail[3];
                    String coordianteString = dataDetail[4] + "," + dataDetail[5];

                    keyText.set(MSID + "|" + sharpTime);
                    resultText.set(baseNum + "|" + coordianteString + "|" + accurateTime);

                    output.collect(keyText, resultText);
                }

            }
        }
    }

    /** completed
     * 比较两个点哪个更靠近整点
     * @param currentPointTime 11:12:00
     * @param newPointTime 11:13:00
     * @return boolean
     */
    private static boolean newtPointIsClosed(String currentPointTime, String newPointTime) {
        try {
            Date currentDate = timeFormat.parse(currentPointTime);
            Date newDate = timeFormat.parse(newPointTime);

            if (newDate.before(currentDate)) {
                return true;
            } else {
                return false;
            }
        } catch (ParseException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Reduce1:
     * 保留一条最靠近整点的数据
     * MSID|整点日期时间    基站编号|经纬度|精确时间
     */
    public static class WashDataReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        private Text resultText = new Text();

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            MostClosedSharpTimePoint bestPoint = new MostClosedSharpTimePoint();

            while (values.hasNext()) {
                String records = values.next().toString();

                String[] baseInfo = records.split("\\|");

                //比较两个点哪个更靠近整点
                if (null == bestPoint.baseNum || newtPointIsClosed(bestPoint.accurateTime, baseInfo[2])) {
                    bestPoint.baseNum = baseInfo[0];
                    bestPoint.coordiantesStr = baseInfo[1];
                    bestPoint.accurateTime = baseInfo[2];
                }
            }

            resultText.set(bestPoint.baseNum + "|" + bestPoint.coordiantesStr + "|" + bestPoint.accurateTime);

            output.collect(key, resultText);
        }
    }
}

class MostClosedSharpTimePoint{
    public String baseNum;
    public String coordiantesStr;
    public String accurateTime;
}