package com.huoteng.baseCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by teng on 12/7/15.
 */
public class MRPersonNumCount {
    /**
     * Map:
     * From:    MSID|整点日期时间    基站编号|经纬度|距离整点秒数,call|距离整点秒数,all
     * To:      基站编号|整点日期时间     人数＝0,call|人数=1,all|经纬度
     */
    public static class PersonCountMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        private Text keyText = new Text();
        private Text result = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String lineData = value.toString();

            String[] keyValue = lineData.split("\\t");
            if (keyValue.length > 1) {
                String[] keyDetail = keyValue[0].split("\\|");
                String[] valueDetail = keyValue[1].split("\\|");

                //取基站编号和整点时间为key
                keyText.set(valueDetail[0] + "|" + keyDetail[1]);

                //格式化distance,如果call事件没有距离人数设为0,all始终设为1
                String[] callInfo = valueDetail[2].split(",");
                String[] allInfo = valueDetail[3].split(",");
                String outputCallInfo;
                try {
                    long callDistance = Long.parseLong(callInfo[0]);
                    outputCallInfo = "1,call";
                } catch (java.lang.NumberFormatException e) {
                    outputCallInfo = "0,call";
                }

                //输出
                result.set(outputCallInfo + "|1,all|" + valueDetail[1]);
                output.collect(keyText, result);
            }
        }
    }

    /**
     * Reduce:
     * 统计:  基站编号|整点日期时间     人数++,call|人数++,all|经纬度
     */
    public static class PersonCountReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            //计数
            int callSum = 0;
            int allSum = 0;
            String coordinateStr = null;
            while (values.hasNext()) {
                String valueStr = values.next().toString();
                String[] valueDetail = valueStr.split("\\|");
                int callNum = Integer.parseInt(valueDetail[0].split(",")[0]);
                int allNum = Integer.parseInt(valueDetail[1].split(",")[0]);

                if (null == coordinateStr) {
                    coordinateStr = valueDetail[1];
                }

                callSum += callNum;
                allSum += allNum;
            }

            if (callSum > allSum) {
                result.set("error,call|error,all|error");
            }
            result.set(callSum + ",call|" + allSum + ",all|" + coordinateStr);
            output.collect(key, result);
        }
    }
}
