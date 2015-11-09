package com.huoteng.mapreduce;

import java.util.ArrayList;
import java.util.Collections;

/**
 *
 * Created by teng on 11/9/15.
 */
public class ReduceStatistics {
    private static final double EARTH_RADIUS = 6378137;

    private ArrayList<Coordinate> historyCoordinates = new ArrayList<Coordinate>();

    /**
     * completed，设定1km为标准
     * @param c1 参照坐标
     * @param c2 当前坐标
     * @return 两点是否足够近
     */

    private static boolean twoPointIsClose(Coordinate c1, Coordinate c2) {

        double lng1 = Double.parseDouble(c1.getLon());
        double lat1 = Double.parseDouble(c1.getLat());
        double lng2 = Double.parseDouble(c2.getLon());
        double lat2 = Double.parseDouble(c2.getLat());

        double distance = getDistance(lng1, lat1, lng2, lat2);

        return 1000 >= distance;
    }


    private static double getDistance(double lng1, double lat1, double lng2, double lat2)
    {
        double radLat1 = rad(lat1);
        double radLat2 = rad(lat2);
        double a = radLat1 - radLat2;
        double b = rad(lng1) - rad(lng2);
        double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a/2),2) +
                Math.cos(radLat1)*Math.cos(radLat2)*Math.pow(Math.sin(b/2),2)));
        s = s * EARTH_RADIUS;
        s = Math.round(s * 10000) / 10000;
        return s;
    }

    private static double rad(double d)
    {
        return d * Math.PI / 180.0;
    }


    /**
     * 对Array按照sum排序
     * @param coordinateArray 需要排序的array
     * @return 排序后的array
     */
    private ArrayList<Coordinate> sortCoordinates(ArrayList<Coordinate> coordinateArray) {

        for (int i = historyCoordinates.size()-1; i >= 0; i--) {
            for (int j = 0; j < i; j++) {
                if (0 == historyCoordinates.get(i).sum) {
                    break;
                } else {
                    if (twoPointIsClose(historyCoordinates.get(i), historyCoordinates.get(j))) {
                        historyCoordinates.get(i).sum += historyCoordinates.get(j).sum;
                        historyCoordinates.get(j).sum = 0;
                    }
                }
            }
        }
        
        ArrayList<Coordinate> tempArray = new ArrayList<Coordinate>();
        for (Coordinate temp : historyCoordinates) {
            if (0 != temp.sum) {
                tempArray.add(temp);
            }
        }
        Collections.sort(tempArray);
        return tempArray;
    }

    public void statisticsTrack(String recordsStr) {

        //将所有记录全部放入history
        String[] recordsArray = recordsStr.split(",");
        for (String i : recordsArray) {
            String[] detail = i.split("\\|");
            int sum = Integer.parseInt(detail[0]);
            historyCoordinates.add(new Coordinate(detail[2], detail[1], sum));
        }
    }

    /**
     * 统计结果返回string
     * 返回最多的前20个
     * @return
     */
    public StringBuffer getResult() {
        /**
         * 将historyCoordinates输出为string
         */

        historyCoordinates = sortCoordinates(historyCoordinates);

        StringBuffer result = new StringBuffer();
//        if (historyCoordinates.size() > 20) {
//            for (int i = 0; i < 20; i++) {
//                Coordinate tmp = historyCoordinates.get(i);
//                result += (tmp.sum + "|" + tmp.getLon() + "|" + tmp.getLat() + ",");
//            }
//        } else {
//            for (int i = 0; i < historyCoordinates.size(); i++) {
//                Coordinate tmp = historyCoordinates.get(i);
//                result += (tmp.sum + "|" + tmp.getLon() + "|" + tmp.getLat() + ",");
//            }
//        }

        for (Coordinate tmp : historyCoordinates) {
            result.append(tmp.sum + "|" + tmp.getLon() + "|" + tmp.getLat() + ",");
        }

        if (result.length() > 0) {
            result.delete(result.length()-1, result.length());
        }

        return result;
    }

}
