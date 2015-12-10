package com.huoteng.placeAnalyzer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

/**
 * 分析用户工作地和居住地MapReduce
 * Created by teng on 12/7/15.
 */
public class PlaceAnalyzerMain {

    public static void main(String[] args) throws Exception {

        JobConf jobEveryDayPointsConf = new JobConf(MRCountPointEveryDay.class);
        jobEveryDayPointsConf.setJobName("CountPointsEveryDay");           //设置一个用户定义的job名称

        jobEveryDayPointsConf.setOutputKeyClass(Text.class);    //为job的输出数据设置Key类
        jobEveryDayPointsConf.setOutputValueClass(Text.class);   //为job输出设置value类

        jobEveryDayPointsConf.setMapperClass(MRCountPointEveryDay.TrackMap.class);         //为job设置Mapper类
        jobEveryDayPointsConf.setCombinerClass(MRCountPointEveryDay.TrackReduce.class);      //为job设置Combiner类
        jobEveryDayPointsConf.setReducerClass(MRCountPointEveryDay.TrackReduce.class);        //为job设置Reduce类
        jobEveryDayPointsConf.setNumReduceTasks(3);             //设置reduce任务的数量

        jobEveryDayPointsConf.setInputFormat(TextInputFormat.class);    //为map-reduce任务设置InputFormat实现类
        jobEveryDayPointsConf.setOutputFormat(TextOutputFormat.class);  //为map-reduce任务设置OutputFormat实现类

        jobEveryDayPointsConf.set("mapred.reduce.child.java.opts", "-Xmx512m");//如果不设置reduce的内存大小heap会炸，原因未知
        Path rawDataInputPath = new Path("big_input");
//        Path rawDataInputPath = new Path("input");//test
        Path placeAnalyzer_middlePath = new Path("middle_workHomePlace");
        placeAnalyzer_middlePath.getFileSystem(jobEveryDayPointsConf).delete(placeAnalyzer_middlePath, true);
        FileInputFormat.setInputPaths(jobEveryDayPointsConf, rawDataInputPath);
        FileOutputFormat.setOutputPath(jobEveryDayPointsConf, placeAnalyzer_middlePath);

        ControlledJob jobEveryDayPoints = new ControlledJob(jobEveryDayPointsConf);

        //测试用
//        JobClient.runJob(jobEveryDayPointsConf);


        JobConf jobWorkHomePlaceCountConf = new JobConf(MRCountWorkHomePlace.class);
        jobWorkHomePlaceCountConf.setJobName("CountHomeWorkPlace");           //设置一个用户定义的job名称

        jobWorkHomePlaceCountConf.setOutputKeyClass(Text.class);    //为job的输出数据设置Key类
        jobWorkHomePlaceCountConf.setOutputValueClass(Text.class);   //为job输出设置value类

        jobWorkHomePlaceCountConf.setMapperClass(MRCountWorkHomePlace.WorkHomePlaceMap.class);         //为job设置Mapper类
        //要在这里禁用Combine
//        jobWorkHomePlaceCountConf.setReducerClass(MRCountWorkHomePlace.WorkHomePlaceReduce.class);        //为job设置Reduce类
        jobWorkHomePlaceCountConf.setNumReduceTasks(3);             //设置reduce任务的数量

        jobWorkHomePlaceCountConf.setInputFormat(TextInputFormat.class);    //为map-reduce任务设置InputFormat实现类
        jobWorkHomePlaceCountConf.setOutputFormat(TextOutputFormat.class);  //为map-reduce任务设置OutputFormat实现类

        jobWorkHomePlaceCountConf.set("mapred.reduce.child.java.opts", "-Xmx512m");
        Path middleInputPath = new Path("middle_workHomePlace");
        Path result_workHomePlacePath = new Path("result_workHomePlace");
        result_workHomePlacePath.getFileSystem(jobWorkHomePlaceCountConf).delete(result_workHomePlacePath, true);
        FileInputFormat.setInputPaths(jobWorkHomePlaceCountConf, middleInputPath);
        FileOutputFormat.setOutputPath(jobWorkHomePlaceCountConf, result_workHomePlacePath);

        //测试用
//        JobClient.runJob(jobWorkHomePlaceCountConf);

        ControlledJob jobWorkHomePlaceCount = new ControlledJob(jobWorkHomePlaceCountConf);
        jobWorkHomePlaceCount.addDependingJob(jobEveryDayPoints);



        JobControl control = new JobControl("WorkHomePlaceAnaylzer");
        control.addJob(jobEveryDayPoints);
        control.addJob(jobWorkHomePlaceCount);

//        control.run();

        //这里有问题，会导致最后的mapreduce任务完毕时程序崩溃，考虑如何修改为自动推出程序
        Thread thread = new Thread(control);
        thread.start();

        while (true) {
            if (control.allFinished()) {
                System.out.println(control.getSuccessfulJobs());
//                thread.stop();
                System.exit(0);
            }
            if (control.getFailedJobs().size() > 0) {
                System.out.println(control.getFailedJobList());
//                thread.stop();
                System.exit(0);
            }
        }
    }
}
