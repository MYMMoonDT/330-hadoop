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
        jobResidentEveryDayConf.setJobName("CountResidentEveryDay");           //����һ���û������job����

        jobResidentEveryDayConf.setOutputKeyClass(Text.class);    //Ϊjob�������������Key��
        jobResidentEveryDayConf.setOutputValueClass(Text.class);   //Ϊjob�������value��

        jobResidentEveryDayConf.setMapperClass(MRResidentEveryDay.Map.class);         //Ϊjob����Mapper��
        jobResidentEveryDayConf.setCombinerClass(MRResidentEveryDay.Reduce.class);      //Ϊjob����Combiner��
        jobResidentEveryDayConf.setReducerClass(MRResidentEveryDay.Reduce.class);        //Ϊjob����Reduce��
        jobResidentEveryDayConf.setNumReduceTasks(3);             //����reduce���������

        jobResidentEveryDayConf.setInputFormat(TextInputFormat.class);    //Ϊmap-reduce��������InputFormatʵ����
        jobResidentEveryDayConf.setOutputFormat(TextOutputFormat.class);  //Ϊmap-reduce��������OutputFormatʵ����

        jobResidentEveryDayConf.set("mapred.reduce.child.java.opts", "-Xmx512m");//���������reduce���ڴ��Сheap��ը��ԭ��δ֪
        Path rawDataInputPath = new Path("big_input");
        Path placeAnalyzer_middlePath = new Path("middle_residentCount");
        placeAnalyzer_middlePath.getFileSystem(jobResidentEveryDayConf).delete(placeAnalyzer_middlePath, true);
        FileInputFormat.setInputPaths(jobResidentEveryDayConf, rawDataInputPath);
        FileOutputFormat.setOutputPath(jobResidentEveryDayConf, placeAnalyzer_middlePath);

        ControlledJob jobResidentEveryDay = new ControlledJob(jobResidentEveryDayConf);

        JobConf jobResidentTotalConf = new JobConf(MRResidentTotal.class);
        jobResidentTotalConf.setJobName("CountResidentTotal");           //����һ���û������job����

        jobResidentTotalConf.setOutputKeyClass(Text.class);    //Ϊjob�������������Key��
        jobResidentTotalConf.setOutputValueClass(Text.class);   //Ϊjob�������value��

        jobResidentTotalConf.setMapperClass(MRResidentTotal.Map.class);         //Ϊjob����Mapper��
        jobResidentTotalConf.setReducerClass(MRResidentTotal.Reduce.class);        //Ϊjob����Reduce��
        jobResidentTotalConf.setNumReduceTasks(3);             //����reduce���������

        jobResidentTotalConf.setInputFormat(TextInputFormat.class);    //Ϊmap-reduce��������InputFormatʵ����
        jobResidentTotalConf.setOutputFormat(TextOutputFormat.class);  //Ϊmap-reduce��������OutputFormatʵ����

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

        //���������⣬�ᵼ������mapreduce�������ʱ�����������������޸�Ϊ�Զ��Ƴ�����
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
