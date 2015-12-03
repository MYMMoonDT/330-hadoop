package com.huoteng.analyzer;

import com.huoteng.mapreduce.Coordinate;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
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
            String dateTimeStr = recordDetail[1];

            String dateStr = new String(dateTimeStr.substring(0, 10));

            keyText.set(MSID);
            resultText.set(dateStr);
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        private Text resultText = new Text();

        /**
         * value样例：time,longitude,latitude|time,longitude,latitude|time,longitude,latitude
         */
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            List<Coordinate> coordinatesList = new ArrayList<Coordinate>();

            while (values.hasNext()) {
                String records = values.next().toString();


            }


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
//        conf.setCombinerClass(Combine.class);      //为job设置Combiner类
        conf.setReducerClass(Reduce.class);        //为job设置Reduce类
        conf.setNumReduceTasks(3);             //设置reduce任务的数量

        conf.setInputFormat(TextInputFormat.class);    //为map-reduce任务设置InputFormat实现类
        conf.setOutputFormat(TextOutputFormat.class);  //为map-reduce任务设置OutputFormat实现类

        conf.set("mapred.reduce.child.java.opts", "-Xmx512m");

        /**
         * InputFormat描述map-reduce中对job的输入定义
         * setInputPaths():为map-reduce job设置路径数组作为输入列表
         * setInputPath()：为map-reduce job设置路径数组作为输出列表
         */

        String[] otherArgs=new String[]{"big_input","middle"}; /* 直接设置输入参数 */
//        String[] testArgs = new String[]{"input", "output_2.0"};
        Path outputPath = new Path(otherArgs[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileInputFormat.setInputPaths(conf, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(conf, outputPath);

        JobClient.runJob(conf);         //运行一个job
    }
}
