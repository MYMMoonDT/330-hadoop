package com.huoteng.util;

import com.huoteng.placeAnalyzer.MRCountPointEveryDay;
import com.huoteng.placeAnalyzer.MRCountWorkHomePlace;
import com.huoteng.placeAnalyzer.ReduceStatistics;
import com.huoteng.statisticsCount.MRCountResultBothWorkHomePlaceNum;
import com.huoteng.statisticsCount.MRCountResultEachWorkHomePlaceNum;
import com.huoteng.statisticsCount.MRSearchResultBothWorkHomePlace;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by bixia on 2015/12/23.
 */
public class JobManager {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        startCountWorkHomePlace(2000);
        startCountEachWorkHomePlaceNum(2000);
        startCountBothWorkHomePlaceNum(2000);
    }

    public static void startCountWorkHomePlace(int distance) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration pointsEveryDayConf = new Configuration();

        Job pointsEveryDayJob = new Job(pointsEveryDayConf, MRCountPointEveryDay.class.getName());
        pointsEveryDayJob.setJarByClass(MRCountPointEveryDay.class);

        pointsEveryDayJob.setMapperClass(MRCountPointEveryDay.TrackMap.class);
        pointsEveryDayJob.setReducerClass(MRCountPointEveryDay.TrackReduce.class);

        pointsEveryDayJob.setNumReduceTasks(3);

        FileInputFormat.setInputPaths(pointsEveryDayJob, new Path("big_input"));

        Path outputPath = new Path("middle_workHomePlace_" + distance);
        outputPath.getFileSystem(pointsEveryDayConf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(pointsEveryDayJob, outputPath);

        pointsEveryDayJob.setOutputKeyClass(Text.class);
        pointsEveryDayJob.setOutputValueClass(Text.class);

        pointsEveryDayJob.waitForCompletion(true);

        Configuration workHomePlaceConf = new Configuration();

        Job workHomePlaceJob = new Job(workHomePlaceConf, MRCountWorkHomePlace.class.getName());
        workHomePlaceJob.setJarByClass(MRCountWorkHomePlace.class);

        workHomePlaceJob.setMapperClass(MRCountWorkHomePlace.WorkHomePlaceMap.class);
        workHomePlaceJob.setReducerClass(MRCountWorkHomePlace.WorkHomePlaceReduce.class);

        workHomePlaceJob.setNumReduceTasks(3);

        FileInputFormat.setInputPaths(workHomePlaceJob, new Path("middle_workHomePlace_" + distance));

        outputPath = new Path("result_workHomePlace_" + distance);
        outputPath.getFileSystem(workHomePlaceConf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(workHomePlaceJob, outputPath);

        workHomePlaceJob.setOutputKeyClass(Text.class);
        workHomePlaceJob.setOutputValueClass(Text.class);

        workHomePlaceJob.waitForCompletion(true);
    }

    public static void startCountEachWorkHomePlaceNum(int distance) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job jobCount = new Job(new Configuration(), MRCountResultEachWorkHomePlaceNum.class.getName());
        jobCount.setJarByClass(MRCountResultEachWorkHomePlaceNum.class);

        jobCount.setMapperClass(MRCountResultEachWorkHomePlaceNum.CountMap.class);
        jobCount.setReducerClass(MRCountResultEachWorkHomePlaceNum.CountReduce.class);

        FileInputFormat.setInputPaths(jobCount, new Path("result_workHomePlace_" + distance));

        Path outputPath = new Path("result_resultEachWorkHomePlaceNum_" + distance);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(jobCount, outputPath);

        jobCount.setOutputKeyClass(Text.class);
        jobCount.setOutputValueClass(IntWritable.class);

        jobCount.waitForCompletion(true);
    }

    public static void startCountBothWorkHomePlaceNum(int distance) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job jobSearch = new Job(new Configuration(), MRSearchResultBothWorkHomePlace.class.getName());
        jobSearch.setJarByClass(MRSearchResultBothWorkHomePlace.class);

        jobSearch.setMapperClass(MRSearchResultBothWorkHomePlace.SearchMap.class);
        jobSearch.setReducerClass(MRSearchResultBothWorkHomePlace.SearchReduce.class);

        FileInputFormat.setInputPaths(jobSearch, new Path("result_workHomePlace_" + distance));

        Path outputPath = new Path("middle_resultBothWorkHomePlaceNum_" + distance);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(jobSearch, outputPath);

        jobSearch.setOutputKeyClass(Text.class);
        jobSearch.setOutputValueClass(IntWritable.class);

        jobSearch.waitForCompletion(true);

        Job jobCount = new Job(new Configuration(), MRCountResultBothWorkHomePlaceNum.class.getName());
        jobCount.setJarByClass(MRCountResultBothWorkHomePlaceNum.class);

        jobCount.setMapperClass(MRCountResultBothWorkHomePlaceNum.CountMap.class);
        jobCount.setReducerClass(MRCountResultBothWorkHomePlaceNum.CountReduce.class);

        FileInputFormat.setInputPaths(jobCount, new Path("middle_resultBothWorkHomePlaceNum_" + distance));

        outputPath = new Path("result_resultBothWorkHomePlaceNum_" + distance);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(jobCount, outputPath);

        jobCount.setOutputKeyClass(Text.class);
        jobCount.setOutputValueClass(IntWritable.class);

        jobCount.waitForCompletion(true);
    }

}
