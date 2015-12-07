package com.huoteng.count;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

/**
 *  统计工作日每天每个整点每个基站的人数
 *  teng on 12/1/15.
 */
public class PersonCountMain {

    /**
     * 连续执行两次mapreduce任务
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        //job1 configuration
        JobConf jobWashDataConf = new JobConf(MRWashData.class);
        jobWashDataConf.setJobName("WashData");           //设置一个用户定义的job名称

        jobWashDataConf.setOutputKeyClass(Text.class);    //为job的输出数据设置Key类
        jobWashDataConf.setOutputValueClass(Text.class);   //为job输出设置value类

        jobWashDataConf.setMapperClass(MRWashData.WashDataMap.class);         //为job设置Mapper类
        jobWashDataConf.setCombinerClass(MRWashData.WashDataReduce.class);      //为job设置Combiner类
        jobWashDataConf.setReducerClass(MRWashData.WashDataReduce.class);        //为job设置Reduce类
        jobWashDataConf.setNumReduceTasks(3);             //设置reduce任务的数量

        jobWashDataConf.setInputFormat(TextInputFormat.class);    //为map-reduce任务设置InputFormat实现类
        jobWashDataConf.setOutputFormat(TextOutputFormat.class);  //为map-reduce任务设置OutputFormat实现类

//        jobWashDataConf.set("mapred.reduce.child.java.opts", "-Xmx512m");

        Path rawDatainput = new Path("big_input");
//        Path inputPath = new Path("input");//test
        Path middle_countPersonNumoutputPath = new Path("middle_CountPersonNum");
        middle_countPersonNumoutputPath.getFileSystem(jobWashDataConf).delete(middle_countPersonNumoutputPath, true);
        FileInputFormat.setInputPaths(jobWashDataConf, rawDatainput);
        FileOutputFormat.setOutputPath(jobWashDataConf, middle_countPersonNumoutputPath);

        ControlledJob jobWashData = new ControlledJob(jobWashDataConf);

        
        //job2 configuration
        JobConf jobCountConf = new JobConf(MRPersonNumCount.class);
        jobCountConf.setJobName("CountSum");

        jobCountConf.setOutputKeyClass(Text.class);
        jobCountConf.setOutputValueClass(IntWritable.class);

        jobCountConf.setMapperClass(MRPersonNumCount.PersonCountMap.class);
        jobCountConf.setCombinerClass(MRPersonNumCount.PersonCountReduce.class);
        jobCountConf.setReducerClass(MRPersonNumCount.PersonCountReduce.class);
        jobCountConf.setNumReduceTasks(3);

        jobCountConf.setInputFormat(TextInputFormat.class);
        jobCountConf.setOutputFormat(TextOutputFormat.class);

        Path middle_countPersonNum = new Path("middle_CountPersonNum");
        Path result_countPersonNum = new Path("result_CountPersonNum");
        result_countPersonNum.getFileSystem(jobCountConf).delete(result_countPersonNum, true);
        FileInputFormat.setInputPaths(jobCountConf, middle_countPersonNum);
        FileOutputFormat.setOutputPath(jobCountConf, result_countPersonNum);

        ControlledJob jobCount = new ControlledJob(jobCountConf);
        jobCount.addDependingJob(jobWashData);

        JobControl control = new JobControl("PersonCount");
        control.addJob(jobWashData);
        control.addJob(jobCount);

        control.run();
    }
}
