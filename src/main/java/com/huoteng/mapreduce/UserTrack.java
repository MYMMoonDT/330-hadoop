package com.huoteng.mapreduce;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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

public class UserTrack {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        private Text resultText = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String lineData = value.toString();

            String[] userTrack = lineData.split("\\|");

            String userMSID = userTrack[0];

            int userState = judgeUserStatus(userTrack[1]);
            String trackValue;

            String userStatus = "";
            Text userKey;

            switch (userState) {
                case UserStatus.NO_NEED:
                    break;
                case UserStatus.WORK_TIME:
                    trackValue = "1|" + userTrack[4] + "|" + userTrack[5];//统一用"|"分割:sum|Longitude|Latitude
                    userStatus += UserStatus.WORK_TIME;
                    userKey = new Text(userMSID + "|" + userStatus);
                    output.collect(userKey, new Text(trackValue));
                    break;
                case UserStatus.HOME_TIME:
                    trackValue = "1|" + userTrack[4] + "|" + userTrack[5];
                    userStatus += UserStatus.HOME_TIME;
                    userKey = new Text(userMSID + "|" + userStatus);
                    resultText.set(trackValue);
                    output.collect(userKey, resultText);
                    break;
                default:
                    break;
            }


        }


        /**
         * completed
         * 判断是否为有效时间
         * @param dateString 需要判断的时间
         * @return bool
         */
        public static int judgeUserStatus(String dateString) {

            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            String date = dateString.substring(0, 11);

            //设置有效时间
            String strHomeStartTime = date + "00:00:00";
            String strHomeEndTime = date + "05:00:00";
            String strWorkStartTime = date + "10:00:00";
            String strWorkEndTime = date + "16:00:00";

            try {
                Date userTime = dateFormat.parse(dateString.substring(0, 19));
                Date homeStartTime = dateFormat.parse(strHomeStartTime);
                Date homeEndTime = dateFormat.parse(strHomeEndTime);
                Date workStartTime = dateFormat.parse(strWorkStartTime);
                Date workEndTime = dateFormat.parse(strWorkEndTime);

                DateFormat weekFormat = new SimpleDateFormat("E");
                String week = weekFormat.format(userTime);

                int result = 0;

                if (week.equals("Sun") || week.equals("Sat")) {
                    result = UserStatus.NO_NEED;
                } else if (userTime.after(homeStartTime) && userTime.before(homeEndTime)) {
                    result = UserStatus.HOME_TIME;
                } else if ((userTime.after(workStartTime) && userTime.before(workEndTime))) {
                    result = UserStatus.WORK_TIME;
                }

                return result;
            } catch (ParseException e) {
                e.printStackTrace();
                return 0;
            }
        }
    }



    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        private Text resultText = new Text();

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            ReduceStatistics statistics = new ReduceStatistics();
            while (values.hasNext()) {
                String recordsStr = values.next().toString();

                statistics.statisticsTrack(recordsStr);
            }

            String result = statistics.getResult().toString();

            if (!result.equals("")) {
                resultText.set(result);
                output.collect(key, resultText);
            }
        }


    }

    public static void main(String[] args) throws Exception
    {
        /**
         * JobConf：map/reduce的job配置类，向hadoop框架描述map-reduce执行的工作
         * 构造方法：JobConf()、JobConf(Class exampleClass)、JobConf(Configuration conf)等
         */
        JobConf conf = new JobConf(UserTrack.class);
        conf.setJobName("UserTrack");           //设置一个用户定义的job名称

        conf.setOutputKeyClass(Text.class);    //为job的输出数据设置Key类
        conf.setOutputValueClass(Text.class);   //为job输出设置value类

        conf.setMapperClass(Map.class);         //为job设置Mapper类
        conf.setCombinerClass(Reduce.class);      //为job设置Combiner类
        conf.setReducerClass(Reduce.class);        //为job设置Reduce类
        conf.setNumReduceTasks(5);             //设置reduce任务的数量

        conf.setInputFormat(TextInputFormat.class);    //为map-reduce任务设置InputFormat实现类
        conf.setOutputFormat(TextOutputFormat.class);  //为map-reduce任务设置OutputFormat实现类

        /**
         * InputFormat描述map-reduce中对job的输入定义
         * setInputPaths():为map-reduce job设置路径数组作为输入列表
         * setInputPath()：为map-reduce job设置路径数组作为输出列表
         */

        String[] otherArgs=new String[]{"big_input","output2"}; /* 直接设置输入参数 */
        Path outputPath = new Path(otherArgs[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileInputFormat.setInputPaths(conf, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(conf, outputPath);

        JobClient.runJob(conf);         //运行一个job
    }
}
