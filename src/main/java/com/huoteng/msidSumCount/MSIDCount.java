package com.huoteng.msidSumCount;

import com.huoteng.placeAnalyzer.Coordinate;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 统计MSID数量
 * Created by teng on 12/3/15.
 */
public class MSIDCount {


    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        private Text keyText = new Text();
        private Text resultText = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String lineData = value.toString();

            String[] recordDetail = lineData.split("\\|");
            String MSID = recordDetail[0];
//            String dateTimeStr = recordDetail[1];

//            String dateStr = new String(dateTimeStr.substring(0, 10));

            keyText.set(MSID);
            resultText.set("1");
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        private Text resultText = new Text();

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            while (values.hasNext()) {
                String records = values.next().toString();

            }

            resultText.set("1");

            output.collect(key, resultText);

        }


    }

    public static void main(String[] args) throws Exception
    {
        /**
         * JobConf：map/reduce的job配置类，向hadoop框架描述map-reduce执行的工作
         * 构造方法：JobConf()、JobConf(Class exampleClass)、JobConf(Configuration conf)等
         */
        JobConf conf = new JobConf(MSIDCount.class);
        conf.setJobName("CountMSID");           //设置一个用户定义的job名称

        conf.setOutputKeyClass(Text.class);    //为job的输出数据设置Key类
        conf.setOutputValueClass(Text.class);   //为job输出设置value类

        conf.setMapperClass(Map.class);         //为job设置Mapper类
        conf.setCombinerClass(Reduce.class);      //为job设置Combiner类
        conf.setReducerClass(Reduce.class);        //为job设置Reduce类
        conf.setNumReduceTasks(3);             //设置reduce任务的数量

        conf.setInputFormat(TextInputFormat.class);    //为map-reduce任务设置InputFormat实现类
        conf.setOutputFormat(TextOutputFormat.class);  //为map-reduce任务设置OutputFormat实现类

        Path inputPath = new Path("big_input");
        Path result_MSIDCountPath = new Path("result_workHomePlace");
        result_MSIDCountPath.getFileSystem(conf).delete(result_MSIDCountPath, true);
        FileInputFormat.setInputPaths(conf, inputPath);
        FileOutputFormat.setOutputPath(conf, result_MSIDCountPath);

        JobClient.runJob(conf);         //运行一个job
    }
}
