package com.huoteng.analyzer;

import com.huoteng.mapreduce.Coordinate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    private static double rad(double d) {
        return d * Math.PI / 180.0;
    }


    /**
     * 统计五个点
     * @param oneDayPoints 需要统计的list
     * @param sum 总数
     * @param countStandard 剩余数量
     * @return 结果
     */
    public static String countPoint(List<Coordinate> oneDayPoints, int sum, int countStandard) {

        if (oneDayPoints.size() >= countStandard) {
            for (int i = 0; i < oneDayPoints.size(); i++) {
                if (null == oneDayPoints.get(i)) {
                    continue;
                }
                for (int j = 0; j < i; j++) {
                    if (null == oneDayPoints.get(j)) {
                        continue;
                    }
                    if (twoPointIsClose(oneDayPoints.get(i), oneDayPoints.get(j))) {
                        oneDayPoints.get(i).sum += oneDayPoints.get(j).sum;
                        oneDayPoints.set(j, null);
                    }
                }
            }


            ArrayList<Coordinate> tmp = new ArrayList<Coordinate>();
            tmp.add(null);
            oneDayPoints.removeAll(tmp);

            String result = "";
            if (oneDayPoints.size() <= (sum + 1 - countStandard)) {
                for (Coordinate point : oneDayPoints) {
                    if (point.sum >= countStandard) {
                        result = point.time + "," + point.getLon() + "," + point.getLat();
                        break;
                    }
                }
            }
            return result;
        } else {
            return "";
        }
    }





//    public void statisticsTrack(String recordsStr) {
//
//        //将所有记录全部放入history
//        String[] recordsArray = recordsStr.split(",");
//        for (String i : recordsArray) {
//            String[] detail = i.split("\\|");
//            int sum = Integer.parseInt(detail[0]);
//            historyCoordinates.add(new Coordinate(detail[2], detail[1], sum));
//        }
//    }


}
