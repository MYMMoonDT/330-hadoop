package com.huoteng.analyzer;

import java.io.*;
import com.huoteng.mapreduce.*;

/**
 * Created by teng on 12/1/15.
 */
public class PlaceStatistics {

    private static void countHomePlace() throws Exception {

        File inputFile = new File("/Users/huoteng/Desktop/output/input.txt");
        String output_homeFileName = "/Users/huoteng/Desktop/output/output_home.txt";
        String output_workFileName = "/Users/huoteng/Desktop/output/output_work.txt";

        FileWriter homeWriter = new FileWriter(output_homeFileName);
        FileWriter workWriter = new FileWriter(output_workFileName);

        InputStream input = new FileInputStream(inputFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));

        while (true) {
            String line = reader.readLine();
//            System.out.println("LINE:" + line);
            if (null == line || line.equals("")) {
                break;
            }


            String[] record = line.split("\\t");

            String placeTypeString = record[0].split("\\|")[1];
            int placeType = Integer.parseInt(placeTypeString);

            String[] placeCoordinate = record[1].split(",");
            StringBuffer resultBuffer = new StringBuffer();
            resultBuffer.append(placeCoordinate[1]);
            resultBuffer.append(" ");
            resultBuffer.append(placeCoordinate[2]);
            resultBuffer.append("\n");
            switch (placeType) {
                case UserStatus.HOME:
                    //将坐标输出到home
                    homeWriter.write(resultBuffer.toString());
                    break;
                case UserStatus.WORK:
                    //将坐标输出到work
                    workWriter.write(resultBuffer.toString());
                    break;
                default:
                    break;
            }
        }

        input.close();
        homeWriter.close();
        workWriter.close();
    }

    public static void main(String[] args) {
        try {
            countHomePlace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
