package com.huoteng.dataCount;

import com.huoteng.placeAnalyzer.Coordinate;
import com.huoteng.placeAnalyzer.UserStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.io.IOException;
import java.util.*;

/**
 * Created by bixia on 2015/12/15.
 */
public class GenerateResult {
    public static class GenerateMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        private Text keyText = new Text();
        private Text resultText = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String lineData = value.toString();
            String[] arrData = lineData.split("\t");

            String msid = arrData[0].split("\\|")[0];
            String time = arrData[0].split("\\|")[1];
            String points = arrData[1];
            if(time.equals("2015-04-16")) {
                keyText.set(msid);
                resultText.set(arrData[0].split("\\|")[2] + "|" + points);
                output.collect(keyText, resultText);
            }
        }
    }

    public static class GenerateReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        private Text resultText = new Text();

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            StringBuffer result = new StringBuffer();
            List<Coordinate> coordinatesList = new ArrayList<Coordinate>();

            while (values.hasNext()) {
                /*String lineData = values.next().toString();
                String[] arrData = lineData.split("\\|");

                for(int i = 1; i < arrData.length; i++) {
                    String[] coordinateArr = arrData[i].split(",");
                    Coordinate coordinate = new Coordinate(coordinateArr[2], coordinateArr[1], Integer.parseInt(coordinateArr[0]));
                    coordinatesList.add(coordinate);
                }*/
                result.append(values.next().toString() + "|");
            }

            if (result.length() > 0) {
                result.deleteCharAt(result.length() - 1);
            }

            /*Collections.sort(coordinatesList);

            for(int i = 0; i < coordinatesList.size(); i++) {
                Coordinate coordinate = coordinatesList.get(i);
                result += parseTime(coordinate.time) + "," + coordinate.getLon() + "," + coordinate.getLat();
                if(i != (coordinatesList.size() - 1)) {
                    result += "|";
                }
            }*/

            resultText.set(result.toString());
            output.collect(key, resultText);
        }

        private String parseTime(int time) {
            if(time < 0) {
                time = time + UserStatus.TIME_24_00;
            }
            int hour = time / 3600;
            int minute = (time % 3600) / 60;
            int second = (time % 3600) % 60;

            return (hour >= 10 ? Integer.toString(hour) : "0" + Integer.toString(hour)) + "," + (minute >= 10 ? Integer.toString(minute) : "0" + Integer.toString(minute)) + "," + (second >= 10 ? Integer.toString(second) : "0" + Integer.toString(second));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf jobConf = new JobConf(GenerateResult.class);
        jobConf.setJobName("GenerateData");

        jobConf.setOutputKeyClass(Text.class);    //为job的输出数据设置Key类
        jobConf.setOutputValueClass(Text.class);   //为job输出设置value类

        jobConf.setMapperClass(GenerateResult.GenerateMap.class);         //为job设置Mapper类
        jobConf.setCombinerClass(GenerateResult.GenerateReduce.class);      //为job设置Combiner类
        jobConf.setReducerClass(GenerateResult.GenerateReduce.class);        //为job设置Reduce类

        jobConf.setInputFormat(TextInputFormat.class);    //为map-reduce任务设置InputFormat实现类
        jobConf.setOutputFormat(TextOutputFormat.class);  //为map-reduce任务设置OutputFormat实现类

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        output.getFileSystem(jobConf).delete(output, true);
        FileInputFormat.setInputPaths(jobConf, input);
        FileOutputFormat.setOutputPath(jobConf, output);

        ControlledJob controlledJob = new ControlledJob(jobConf);

        JobControl control = new JobControl("GenerateResult");
        control.addJob(controlledJob);

        Thread thread = new Thread(control);
        thread.start();

        while (true) {
            if (control.allFinished()) {
                System.out.println(control.getSuccessfulJobList());
                System.exit(0);
            }
            if (control.getFailedJobList().size() > 0) {
                System.out.println(control.getFailedJobList());
                System.exit(0);
            }
        }
    }
}
