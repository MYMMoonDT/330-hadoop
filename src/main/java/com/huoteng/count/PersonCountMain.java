package com.huoteng.count;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.FileOutputStream;

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
    public static void main(String[] args) throws Exception
    {
        /**
         * JobjobWashDataConf：map/reduce的job配置类，向hadoop框架描述map-reduce执行的工作
         * 构造方法：JobjobWashDataConf()、JobjobWashDataConf(Class exampleClass)、JobjobWashDataConf(jobWashDataConfiguration jobWashDataConf)等
         */
        //job1
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

        /**
         * InputFormat描述map-reduce中对job的输入定义
         * setInputPaths():为map-reduce job设置路径数组作为输入列表
         * setInputPath()：为map-reduce job设置路径数组作为输出列表
         */

        Path inputPath = new Path("big_input");
//        Path inputPath = new Path("test");//test
        Path outputPath = new Path("middle_CountPersonNum");
        outputPath.getFileSystem(jobWashDataConf).delete(outputPath, true);
        FileInputFormat.setInputPaths(jobWashDataConf, inputPath);
        FileOutputFormat.setOutputPath(jobWashDataConf, outputPath);

        JobClient.runJob(jobWashDataConf);



        //job2
        JobConf jobCount = new JobConf(MRPersonNumCount.class);
        jobCount.setJobName("CountSum");

        jobCount.setOutputKeyClass(Text.class);
        jobCount.setOutputValueClass(IntWritable.class);

        jobCount.setMapperClass(MRPersonNumCount.PersonCountMap.class);
        jobCount.setCombinerClass(MRPersonNumCount.PersonCountReduce.class);
        jobCount.setReducerClass(MRPersonNumCount.PersonCountReduce.class);
        jobCount.setNumReduceTasks(3);

        jobCount.setInputFormat(TextInputFormat.class);
        jobCount.setOutputFormat(TextOutputFormat.class);

        inputPath = new Path("middle_CountPersonNum");
        outputPath = new Path("result_CountPersonNum");
        outputPath.getFileSystem(jobWashDataConf).delete(outputPath, true);
        FileInputFormat.setInputPaths(jobCount, inputPath);
        FileOutputFormat.setOutputPath(jobCount, outputPath);

        JobClient.runJob(jobCount);
    }
}
