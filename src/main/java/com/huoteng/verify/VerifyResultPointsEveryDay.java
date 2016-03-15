package com.huoteng.verify;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.zookeeper.txn.Txn;

import java.io.IOException;

/**
 * Created by bixia on 2015/12/18.
 */
public class VerifyResultPointsEveryDay {
    public static class VerifyMap extends Mapper<LongWritable, Text, Text, Text> {

        private Text keyText = new Text();
        private Text resultText = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String lineData = value.toString();

            String[] userTrack = lineData.split("\t");

            if(userTrack.length == 2) {
                String keyDatePlaceString = userTrack[0];
                String track = userTrack[1];

                String[] userMSIDDatePlace = keyDatePlaceString.split("\\|");

                String date = userMSIDDatePlace[1];

                if(date.equals("2015-04-17")) {
                    keyText.set(userMSIDDatePlace[0] + "|" + date + "|" + userMSIDDatePlace[2]);
                    resultText.set(track);
                    context.write(keyText, resultText);
                }
            }

        }
    }

    public static class VerifyReduce extends Reducer<Text, Text, Text, Text> {

        private Text resultText = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = new Job(new Configuration(), VerifyResultPointsEveryDay.class.getName());
        job.setJarByClass(VerifyResultPointsEveryDay.class);

        job.setMapperClass(VerifyResultPointsEveryDay.VerifyMap.class);
        job.setReducerClass(VerifyResultPointsEveryDay.VerifyReduce.class);

        job.setNumReduceTasks(3);

        FileInputFormat.setInputPaths(job, new Path("middle_workHomePlace"));

        Path outputPath = new Path("verify_pointsEveryDay");
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}
