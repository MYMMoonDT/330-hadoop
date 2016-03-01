package com.huoteng.verify;

import com.huoteng.placeAnalyzer.Coordinate;
import com.huoteng.placeAnalyzer.ReduceStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bixia on 2015/12/18.
 */
public class VerifyResultWorkHomePlace {
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

                String[] tracksArray = track.split("\\|");

                List<Coordinate> oneDayCoordinates = new ArrayList<Coordinate>();

                for (String point : tracksArray) {
                    if (!point.equals("")) {
                        String[] pointDetail = point.split(",");

                        int time = Integer.parseInt(pointDetail[0]);
                        oneDayCoordinates.add(new Coordinate(pointDetail[2], pointDetail[1], time));
                    }
                }

                String mapResult = ReduceStatistics.countPoint(oneDayCoordinates, 3);

                if(!mapResult.equals("")) {
                    String[] userMSIDDatePlace = keyDatePlaceString.split("\\|");

                    String date = userMSIDDatePlace[1];

                    if (date.equals("2015-04-17")) {
                        keyText.set(userMSIDDatePlace[0] + "|" + date + "|" + userMSIDDatePlace[2]);
                        resultText.set(mapResult);
                        context.write(keyText, resultText);
                    }
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

        Job job = new Job(new Configuration(), VerifyResultWorkHomePlace.class.getName());
        job.setJarByClass(VerifyResultWorkHomePlace.class);

        job.setMapperClass(VerifyMap.class);
        job.setReducerClass(VerifyReduce.class);

        job.setNumReduceTasks(3);

        FileInputFormat.setInputPaths(job, new Path("middle_workHomePlace"));

        Path outputPath = new Path("verify_workHomePlace");
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}
