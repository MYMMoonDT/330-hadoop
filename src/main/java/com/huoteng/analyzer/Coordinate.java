package com.huoteng.analyzer;

import java.io.*;

/**
 * Created by teng on 11/26/15.
 */
public class Coordinate {

    /**
     * 读原始数据文件
     */
    public static void readRawFile() {
        File inputFile = new File("/Users/huoteng/Desktop/output/output_aa");
        File outputFile = new File("/Users/huoteng/Desktop/output/coordinate");

        if (outputFile.exists()) {
            outputFile.delete();
        }

        try {
            if (inputFile.canRead()) {
                outputFile.createNewFile();
                FileOutputStream output = new FileOutputStream(outputFile);
                BufferedReader reader = new BufferedReader(new FileReader(inputFile));
                String tmpReaderString = null;
                String[] tmpArray1 = null;
                String[] tmpArray2 = null;
                while ((tmpReaderString = reader.readLine()) != null) {

                    tmpArray1 = tmpReaderString.split("\\s");
                    String tmpString = tmpArray1[1];
                    tmpArray2 = tmpString.split("\\|");
                    for (String i : tmpArray2) {
                        String[] tmp = i.split(",");

                        //数据不完整时会出现数组越界
                        String result = tmp[0] + " " + tmp[1] + " " + tmp[2] + "\n";
                        output.write(result.getBytes());
                    }
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        readRawFile();
    }
}
