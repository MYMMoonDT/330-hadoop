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
     * From:    MSID|整点日期时间    基站编号|经纬度|精确时间
     * To:      基站编号|经纬度|整点日期时间     人数＝1
     */
    public static class PersonCountMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        private Text keyText = new Text();
        private IntWritable result = new IntWritable();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String lineData = value.toString();

            String[] keyValue = lineData.split("\\t");
            if (keyValue.length > 1) {
                String[] keyDetail = keyValue[0].split("\\|");
                String[] valueDetail = keyValue[1].split("\\|");

                keyText.set(valueDetail[0] + "|" + valueDetail[1] + "|" + keyDetail[1]);
                result.set(1);

                output.collect(keyText, result);
            }
        }
    }

    /**
     * Reduce:
     * 统计:  基站编号|经纬度|整点日期时间     人数++
     */
    public static class PersonCountReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }

            result.set(sum);
            output.collect(key, result);
        }
    }
}
