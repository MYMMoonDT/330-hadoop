package com.huoteng.baseCount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
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

        Path rawDataInput = new Path("big_input");
//        Path rawDataInput = new Path("input");//test
        Path middle_countPersonNumOutputPath = new Path("middle_CountPersonNum");
        middle_countPersonNumOutputPath.getFileSystem(jobWashDataConf).delete(middle_countPersonNumOutputPath, true);
        FileInputFormat.setInputPaths(jobWashDataConf, rawDataInput);
        FileOutputFormat.setOutputPath(jobWashDataConf, middle_countPersonNumOutputPath);

        ControlledJob jobWashData = new ControlledJob(jobWashDataConf);

        
        //job2 configuration
        JobConf jobCountConf = new JobConf(MRPersonNumCount.class);
        jobCountConf.setJobName("CountSum");

        jobCountConf.setOutputKeyClass(Text.class);
        jobCountConf.setOutputValueClass(Text.class);

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

        //这里有问题，会导致最后的mapreduce任务完毕时程序崩溃，考虑如何修改为自动推出程序
//        Thread thread = new Thread(control);
//        thread.start();
//        while (true) {
//            if (control.allFinished()) {
//                System.out.println(control.getSuccessfulJobList());
////                control.stop();
//                System.exit(0);
//            }
//            if (control.getFailedJobList().size() > 0) {
//                System.out.println(control.getFailedJobList());
////                control.stop();
//                System.exit(0);
//            }
//        }
    }
}
