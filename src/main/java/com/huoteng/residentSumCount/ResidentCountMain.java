package com.huoteng.residentSumCount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

/**
 * Created by bixia on 2015/12/16.
 */
public class ResidentCountMain {

    public static void main(String[] args) throws Exception {
        JobConf jobResidentEveryDayConf = new JobConf(MRResidentEveryDay.class);
        jobResidentEveryDayConf.setJobName("CountResidentEveryDay");           //设置一个用户定义的job名称

        jobResidentEveryDayConf.setOutputKeyClass(Text.class);    //为job的输出数据设置Key类
        jobResidentEveryDayConf.setOutputValueClass(Text.class);   //为job输出设置value类

        jobResidentEveryDayConf.setMapperClass(MRResidentEveryDay.Map.class);         //为job设置Mapper类
        jobResidentEveryDayConf.setCombinerClass(MRResidentEveryDay.Reduce.class);      //为job设置Combiner类
        jobResidentEveryDayConf.setReducerClass(MRResidentEveryDay.Reduce.class);        //为job设置Reduce类
        jobResidentEveryDayConf.setNumReduceTasks(3);             //设置reduce任务的数量

        jobResidentEveryDayConf.setInputFormat(TextInputFormat.class);    //为map-reduce任务设置InputFormat实现类
        jobResidentEveryDayConf.setOutputFormat(TextOutputFormat.class);  //为map-reduce任务设置OutputFormat实现类

        jobResidentEveryDayConf.set("mapred.reduce.child.java.opts", "-Xmx512m");//如果不设置reduce的内存大小heap会炸，原因未知
        Path rawDataInputPath = new Path("big_input");
        Path placeAnalyzer_middlePath = new Path("middle_residentCount");
        placeAnalyzer_middlePath.getFileSystem(jobResidentEveryDayConf).delete(placeAnalyzer_middlePath, true);
        FileInputFormat.setInputPaths(jobResidentEveryDayConf, rawDataInputPath);
        FileOutputFormat.setOutputPath(jobResidentEveryDayConf, placeAnalyzer_middlePath);

        ControlledJob jobResidentEveryDay = new ControlledJob(jobResidentEveryDayConf);

        JobConf jobResidentTotalConf = new JobConf(MRResidentTotal.class);
        jobResidentTotalConf.setJobName("CountResidentTotal");           //设置一个用户定义的job名称

        jobResidentTotalConf.setOutputKeyClass(Text.class);    //为job的输出数据设置Key类
        jobResidentTotalConf.setOutputValueClass(Text.class);   //为job输出设置value类

        jobResidentTotalConf.setMapperClass(MRResidentTotal.Map.class);         //为job设置Mapper类
        jobResidentTotalConf.setReducerClass(MRResidentTotal.Reduce.class);        //为job设置Reduce类
        jobResidentTotalConf.setNumReduceTasks(3);             //设置reduce任务的数量

        jobResidentTotalConf.setInputFormat(TextInputFormat.class);    //为map-reduce任务设置InputFormat实现类
        jobResidentTotalConf.setOutputFormat(TextOutputFormat.class);  //为map-reduce任务设置OutputFormat实现类

        jobResidentTotalConf.set("mapred.reduce.child.java.opts", "-Xmx512m");
        Path middleInputPath = new Path("middle_residentCount");
        Path result_workHomePlacePath = new Path("result_residentCount");
        result_workHomePlacePath.getFileSystem(jobResidentTotalConf).delete(result_workHomePlacePath, true);
        FileInputFormat.setInputPaths(jobResidentTotalConf, middleInputPath);
        FileOutputFormat.setOutputPath(jobResidentTotalConf, result_workHomePlacePath);

        ControlledJob jobResidentTotal = new ControlledJob(jobResidentTotalConf);
        jobResidentTotal.addDependingJob(jobResidentEveryDay);

        JobControl control = new JobControl("ResidentCompute");
        control.addJob(jobResidentEveryDay);
        control.addJob(jobResidentTotal);

        //这里有问题，会导致最后的mapreduce任务完毕时程序崩溃，考虑如何修改为自动推出程序
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
