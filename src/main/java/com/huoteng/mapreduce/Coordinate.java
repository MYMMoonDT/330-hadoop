package com.huoteng.mapreduce;

/**
 * 坐标类
 * Created by teng on 11/5/15.
 */
public class Coordinate implements Comparable<Coordinate> {
    private String lat;
    private String lon;
    public int sum;

    public Coordinate(String lat, String lon, int sum) {
        this.sum = sum;
        this.lat = lat;
        this.lon = lon;
    }

    public String getLat() {
        return lat;
    }

    public String getLon() {
        return lon;
    }

    public int compareTo(Coordinate temp) {
        return temp.sum - this.sum;
    }
}