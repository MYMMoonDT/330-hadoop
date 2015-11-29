package com.huoteng.mapreduce;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 *
 * Created by teng on 11/5/15.
 */
public class UserStatus {
    static public final int WEEKEND = 3;
    static public final int WEEKDAY = 2;
    static public final int NO_NEED = 1;

    static public final int TIME_00_00 = 86400;
    static public final int TIME_01_00 = 3600;
    static public final int TIME_02_00 = 7200;
    static public final int TIME_03_00 = 10800;
    static public final int TIME_04_00 = 14400;
    static public final int TIME_10_00 = 36000;
    static public final int TIME_11_00 = 39600;
    static public final int TIME_14_00 = 50400;
    static public final int TIME_15_00 = 54000;
    static public final int TIME_16_OO = 57600;
    static public final int TIME_20_00 = 72000;

    private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static DateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
    private static DateFormat weekFormat = new SimpleDateFormat("E");

    static {
        timeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    }
    /**
//     * completed
//     * 判断是否为有效时间
//     * @param dateString 需要判断的时间
//     * @return bool
//     */
//    public static int judgeUserStatus(String dateString) {
//
//        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//
//        String date = dateString.substring(0, 11);
//
//        //设置有效时间
//        String strHomeStartTime = date + "00:00:00";
//        String strHomeEndTime = date + "05:00:00";
//        String strWorkStartTime = date + "10:00:00";
//        String strWorkEndTime = date + "16:00:00";
//
//        try {
//            Date userTime = dateFormat.parse(dateString.substring(0, 19));
//            Date homeStartTime = dateFormat.parse(strHomeStartTime);
//            Date homeEndTime = dateFormat.parse(strHomeEndTime);
//            Date workStartTime = dateFormat.parse(strWorkStartTime);
//            Date workEndTime = dateFormat.parse(strWorkEndTime);
//
//            DateFormat weekFormat = new SimpleDateFormat("E");
//            String week = weekFormat.format(userTime);
//
//            int result = 0;
//
//            if (week.equals("Sun") || week.equals("Sat")) {
//                result = UserStatus.NO_NEED;
//            } else if (userTime.after(homeStartTime) && userTime.before(homeEndTime)) {
//                result = UserStatus.HOME_TIME;
//            } else if ((userTime.after(workStartTime) && userTime.before(workEndTime))) {
//                result = UserStatus.WORK_TIME;
//            }
//
//            return result;
//        } catch (ParseException e) {
//            e.printStackTrace();
//            return 0;
//        }
//    }


    /** need test
     *
     * @param dateTimeString 用户当前日期时间字符串
     * @return 用户当前时间字符串
     */
    public static String getUserTime(String dateTimeString) {
        String userTime = new String(dateTimeString.substring(11, 19));

        //需要把时间转换为int
        try {
            Date time = timeFormat.parse(userTime);
            int timeSecondNum = new Long(time.getTime()).intValue();

            return timeSecondNum + "";
        } catch (ParseException e) {
            e.printStackTrace();

            return null;
        }
    }

    /** need test
     * 判断时间是否需要
     * @param userDateTimeString 用户日期时间String
     * @return 是需要的时间段返回true
     */
    public static boolean judgeTimeValid(String userDateTimeString) {

        //判断时间是否有效        对于一天来说，共有<=5个休息地原始记录分别对应0:00, 1:00, 2:00, 3:00, 4:00      共有<=5个工作地原始记录分别对应10:00, 11:00, 14:00, 15:00, 16:00

//        (2) 计算5个点半径1km内包含另外4个点有多少个，包括自身>=3个，该点通过测试条件，作为当天备选点
//                (3) 当天备选点中包含其他点个数最多的点确定为当天的工作地点和休息地点，包含数量相同的情况下随机选择其中一个
//                (4) 10个工作日可以确定<=10个工作地点和<=10个休息地点，从10个点中找到半径1km内包含另外点有多少个，包括自身>=6个，通过测试
//                (5) 选取包含点数最多的备选点作为最后工作地和休息地结果

        //这次mapreduce任务只做工作日的
        String userDate = new String(userDateTimeString.substring(0, 10));
        boolean result = false;

        try {
            Date userDateTime = dateFormat.parse(userDate);
            String week = weekFormat.format(userDateTime);

            System.out.println("WEEK:" + week);
            if (!(week.equals("Sun") || week.equals("Sat"))) {
                String userTimeString = getUserTime(userDateTimeString);
                int userTime = Integer.parseInt(userTimeString);

                if ((userTime < TIME_04_00) || (userTime > TIME_10_00 && userTime < TIME_16_OO) || (userTime > TIME_20_00)) {
                    result = true;
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return result;
    }

    /** uncomplete
     * 根据要求得到在家的时间点
     * @param homeTimeCoordinates 已经按时间排好顺序，所有时间点的List
     * @return time,longitude,latitude|time,longitude,latitude|......
     */
    public static String getHomeTimePoint(List<Coordinate> homeTimeCoordinates) {




        return "";
    }

    /** uncomplete
     * 根据要求得到在工作地的时间点
     * @param workTimeCoordinates 已经按时间排好顺序，所有时间点的List
     * @return time,longitude,latitude|time,longitude,latitude|......
     */
    public static String getWorkTimePoint(List<Coordinate> workTimeCoordinates) {

        return "";
    }
}
