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
     * From:    MSID|整点日期时间    基站编号|经纬度|距离整点秒数|事件类型
     * To:      基站编号|事件类型|整点日期时间     人数＝1|经纬度
     */
    public static class PersonCountMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        private Text keyText = new Text();
        private Text result = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String lineData = value.toString();


            //取基站编号和时间以及事件类型为key
            //人数计为1,同时记录经纬度

            String[] keyValue = lineData.split("\\t");
            if (keyValue.length > 1) {
                String[] keyDetail = keyValue[0].split("\\|");
                String[] valueDetail = keyValue[1].split("\\|");

                keyText.set(valueDetail[0] + "|" + valueDetail[3] + "|" + keyDetail[1]);
                result.set("1" + "|" + valueDetail[1]);

                output.collect(keyText, result);
            }
        }
    }

    /**
     * Reduce:
     * 统计:  基站编号|经纬度|整点日期时间     人数++
     */
    public static class PersonCountReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            //计数

            int sum = 0;
            String coordinateStr = null;
            while (values.hasNext()) {
                String valueStr = values.next().toString();
                String[] valueDetail = valueStr.split("\\|");
                int number = Integer.parseInt(valueDetail[0]);

                if (null == coordinateStr) {
                    coordinateStr = valueDetail[1];
                }

                sum += number;
            }

            result.set(sum + "|" + coordinateStr);
            output.collect(key, result);
        }
    }
}
