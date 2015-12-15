package com.huoteng.placeAnalyzer;

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

    public static boolean twoPointIsClose(Coordinate c1, Coordinate c2) {

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
     * @param countStandard 剩余数量
     * @return 结果
     */
    public static String countPoint(List<Coordinate> oneDayPoints, int countStandard) {

        if (oneDayPoints.size() >= countStandard) {
            for (int i = 0; i < oneDayPoints.size(); i++) {
                for (int j = 0; j < oneDayPoints.size(); j++) {
                    if ( i == j) {
                        continue;
                    }
                    if (twoPointIsClose(oneDayPoints.get(i), oneDayPoints.get(j))) {
                        oneDayPoints.get(i).sum++;
                    }
                }
            }

            String result = "";
            Coordinate max = oneDayPoints.get(0);
            for (Coordinate point : oneDayPoints) {
                if (point.sum > max.sum) {
                    max = point;
                }
            }

            // 选择最可能点的逻辑更改：如果包含的其他点个数相同，还要比较到其他点的距离总和，最小的胜出
            List<Coordinate> maxList = new ArrayList<Coordinate>();
            maxList.add(max);
            for (Coordinate point : oneDayPoints) {
                if (point.sum == max.sum && point != max) {
                    maxList.add(point);
                }
            }

            if(maxList.size() > 1) {
                for(int i = 0; i < maxList.size(); i++) {
                    Coordinate maxCoordinate = maxList.get(i);
                    maxCoordinate.distance = 0.0;
                    for (Coordinate point : oneDayPoints) {
                        double lng1 = Double.parseDouble(point.getLon());
                        double lat1 = Double.parseDouble(point.getLat());
                        double lng2 = Double.parseDouble(maxCoordinate.getLon());
                        double lat2 = Double.parseDouble(maxCoordinate.getLat());

                        maxCoordinate.distance += getDistance(lng1, lat1, lng2, lat2);
                    }
                }
                max = maxList.get(0);
                for(Coordinate point : maxList) {
                    if(point != max && point.distance < max.distance) {
                        max = point;
                    }
                }
            }

            if (max.sum >= countStandard) {
                result = max.time + "," + max.getLon() + "," + max.getLat();
            }

            return result;
        } else {
            return "";
        }
    }
}
