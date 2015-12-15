package com.huoteng.dataCount;

import com.huoteng.placeAnalyzer.Coordinate;
import com.huoteng.placeAnalyzer.UserStatus;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;

/**
 * 用于z数据验证
 * Created by teng on 12/10/15.
 */
public class ResultVerify {

    //找到2015-04-16的所有数据
    public static void main(String[] args) throws Exception {
        FileReader reader = new FileReader("/Users/huoteng/Desktop/output/result/result12.14.txt");
        BufferedReader bufferedReader = new BufferedReader(reader);

        BufferedWriter output3 = new BufferedWriter(new FileWriter("/Users/huoteng/Desktop/output/Result12.14.txt"));
        BufferedWriter output14 = new BufferedWriter(new FileWriter("/Users/huoteng/Desktop/output/result14.txt"));

        String tmp = bufferedReader.readLine();

        while (null != tmp) {
//            String[] detail = tmp.split("\\|");

            //匹配每一行，找到16号的数据
//            if (detail[1].equals("2015-04-16")) {
                //找到16的数据找出3点和14点的数据
                tmp = tmp.replace("|", ",");
                tmp = tmp.replace("\t", ",");

                output3.write(tmp + "\n");
//                String[] keyValue = tmp.split("\\t");
//                if (keyValue.length > 1) {
//                    int  placeSatus = Integer.parseInt(keyValue[0].split("\\|")[2]);
//                    String[] values = keyValue[1].split("\\|");
//
//                    ArrayList<Coordinate> coordinates = new ArrayList<Coordinate>();
//                    for (String value : values) {
//                        String[] point = value.split(",");
//                        coordinates.add(new Coordinate(point[2], point[1], Integer.parseInt(point[0])));
//                    }
//
//                    Collections.sort(coordinates);
//
//                    switch (placeSatus) {
//                        case UserStatus.HOME:
//                            for (int i = 0; i < coordinates.size(); i++) {
//                                if (coordinates.get(0).time > UserStatus.TIME_03_00) {
//                                    break;
//                                }
//                                if (coordinates.get(i).time > UserStatus.TIME_03_00 && 0 != i) {
//                                    output3.write(detail[0] + "|3点|" + coordinates.get(i-1).time + "," + coordinates.get(i-1).getLon() + "," + coordinates.get(i-1).getLat());
//                                    output3.write("\n");
//                                    break;
//                                }
//                                if (i == coordinates.size()-1 && coordinates.get(i).time <= UserStatus.TIME_03_00) {
//                                    output3.write(detail[0] + "|3点|" + coordinates.get(i).time + "," + coordinates.get(i).getLon() + "," + coordinates.get(i).getLat());
//                                    output3.write("\n");
//                                }
//                            }
//                            break;
//
//                        case UserStatus.WORK:
//                            for (int i = 0; i < coordinates.size(); i++) {
//                                if (coordinates.get(0).time > UserStatus.TIME_14_00) {
//                                    break;
//                                }
//                                if (coordinates.get(i).time > UserStatus.TIME_14_00 && 0 != i) {
//                                    output14.write(detail[0] + "|14点|" + coordinates.get(i-1).time + "," + coordinates.get(i-1).getLon() + "," + coordinates.get(i-1).getLat());
//                                    output14.write("\n");
//                                    break;
//                                }
//                                if (i == coordinates.size()-1 && coordinates.get(i).time <= UserStatus.TIME_14_00) {
//                                    output14.write(detail[0] + "|14点|" + coordinates.get(i).time + "," + coordinates.get(i).getLon() + "," + coordinates.get(i).getLat());
//                                    output14.write("\n");
//                                }
//                            }
//                            break;
//                    }
//                }
//            }
            tmp = bufferedReader.readLine();
        }

        output3.close();
        output14.close();
        bufferedReader.close();
        reader.close();

    }
}
