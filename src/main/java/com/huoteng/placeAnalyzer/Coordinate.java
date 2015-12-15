package com.huoteng.placeAnalyzer;

/**
 * 坐标类
 * Created by teng on 11/5/15.
 */
public class Coordinate implements Comparable<Coordinate> {
    private String lat;
    private String lon;
    public int time;
    public int sum;
    public double distance;

    public Coordinate(String lat, String lon, int time) {
        this.time = time;
        this.lat = lat;
        this.lon = lon;
        sum = 1;
        distance = 0.0;
    }

    public String getLat() {
        return lat;
    }

    public String getLon() {
        return lon;
    }

    //need test
    public int compareTo(Coordinate temp) {
        //按时间先后排序，时间为int值
        return this.time - temp.time;
    }
}