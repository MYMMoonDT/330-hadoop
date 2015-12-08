package com.huoteng.dataCount;

import com.huoteng.placeAnalyzer.UserStatus;

import java.io.*;
import java.util.ArrayList;

/**
 * 数据验证
 * Created by teng on 12/8/15.
 */
public class DataCount {


//    FileReader reader = new FileReader("D:\\lbhdev\\feisanWeb\\src\\265_url.txt");
//    BufferedReader br = new BufferedReader(reader);
//    String s1 = null;
//    while((s1 = br.readLine()) != null) {
//    }
//    br.close();
//    reader.close()



    //找到2015-04-16的所有数据
    public static void main(String[] args) throws Exception {
        FileReader reader = new FileReader("/Users/huoteng/Desktop/output/middle/middle.txt");
        BufferedReader bufferedReader = new BufferedReader(reader);

        BufferedWriter output3 = new BufferedWriter(new FileWriter("/Users/huoteng/Desktop/output/result3.txt"));
        BufferedWriter output14 = new BufferedWriter(new FileWriter("/Users/huoteng/Desktop/output/result14.txt"));

        String tmp = bufferedReader.readLine();

        while (null != tmp) {
            String[] detail = tmp.split("\\|");

            //匹配每一行，找到16号的数据
            if (detail[1].equals("2015-04-16")) {
                //找到16的数据找出3点和14点的数据
                String[] keyValue = tmp.split("\\t");
                if (keyValue.length > 1) {
                    int  placeSatus = Integer.parseInt(keyValue[0].split("\\|")[2]);
                    String[] values = keyValue[1].split("\\|");

                    ArrayList<Integer> times = new ArrayList<Integer>();
                    for (int i = 0; i < values.length; i++) {
                        times.add(Integer.parseInt(values[i].split(",")[0]));
                    }

                    switch (placeSatus) {
                        case UserStatus.HOME:
                            for (int i = 0; i < times.size(); i++) {
                                if (times.get(i) > UserStatus.TIME_03_00 && 0 != i) {
                                    output3.write(detail[0] + "|3点|" + values[i-1]);
                                    output3.write("\n");
                                }
                            }
                            break;

                        case UserStatus.WORK:
                            for (int i = 0; i < times.size(); i++) {
                                if (times.get(i) > UserStatus.TIME_14_00 && 0 != i) {
                                    output14.write(detail[0] + "|14点|" + values[i-1]);
                                    output14.write("\n");
                                    break;
                                }
                            }
                            break;
                    }
                }
            }
            tmp = bufferedReader.readLine();
        }

        output3.close();
        output14.close();
        bufferedReader.close();
        reader.close();

    }
}
