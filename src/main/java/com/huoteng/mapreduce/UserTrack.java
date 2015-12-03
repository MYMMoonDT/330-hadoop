package com.huoteng.mapreduce;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * 统计每个人的家和工作的可疑点
 * Created by teng on 12/1/15.
 */

public class UserTrack {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

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
                if (UserStatus.HOME == userPlace && userTime > UserStatus.TIME_16_OO) {
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



    public static class Combine extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

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
                String worksPointString = UserStatus.getWorkTimePoint(coordinatesList);
                resultText.set(worksPointString);
            }
            output.collect(key, resultText);
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
                String worksPointString = UserStatus.getWorkTimePoint(coordinatesList);
                resultText.set(worksPointString);
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
        JobConf conf = new JobConf(UserTrack.class);
        conf.setJobName("GetUserPoint");           //设置一个用户定义的job名称

        conf.setOutputKeyClass(Text.class);    //为job的输出数据设置Key类
        conf.setOutputValueClass(Text.class);   //为job输出设置value类

        conf.setMapperClass(Map.class);         //为job设置Mapper类
        conf.setCombinerClass(Combine.class);      //为job设置Combiner类
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
