package com.huoteng.placeAnalyzer;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 *
 * Created by teng on 11/5/15.
 */
public class UserStatus {

//    static public final int WEEKEND = 3;
//    static public final int WEEKDAY = 2;
//    static public final int NO_NEED = 1;

    static public final int HOME = 4;
    static public final int WORK = 5;

    static public final int TIME_00_00 = 0;
    static public final int TIME_01_00 = 3600;
    static public final int TIME_02_00 = 7200;
    static public final int TIME_03_00 = 10800;
    static public final int TIME_04_00 = 14400;
    static public final int TIME_08_50 = 31800;
    static public final int TIME_10_00 = 36000;
    static public final int TIME_11_00 = 39600;
    static public final int TIME_14_00 = 50400;
    static public final int TIME_15_00 = 54000;
    static public final int TIME_16_OO = 57600;
    static public final int TIME_20_00 = 72000;
    static public final int TIME_24_00 = 86400;

    private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static DateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
    private static DateFormat weekFormat = new SimpleDateFormat("E");

    static {
        timeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    public static String judgeUserPlace(String dateTimeString) {
        String userTime = new String(dateTimeString.substring(11, 19));
        StringBuffer isValidTime = new StringBuffer();

        try {
            Date time = timeFormat.parse(userTime);
            Date time_04_00 = timeFormat.parse("04:00:01");
            Date time_08_50 = timeFormat.parse("08:49:59");
            Date time_16_00 = timeFormat.parse("16:00:01");
            Date time_20_00 = timeFormat.parse("19:59:59");

            if (time.before(time_04_00) || time.after(time_20_00)) {
                isValidTime.append(HOME);
            } else if (time.after(time_08_50) && time.before(time_16_00)) {
                isValidTime.append(WORK);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return isValidTime.toString();
    }

    /**
     * completed
     * 将时间转换为当天秒数返回
     *
     * @param dateTimeString 用户当前日期时间字符串
     * @return 用户当前时间字符串
     */
    public static int getUserTime(String dateTimeString) {
        String userTime = new String(dateTimeString.substring(11, 19));

        //需要把时间转换为int
        try {
            Date time = timeFormat.parse(userTime);
//            System.out.println("dataTime" + time);
            int timeSecondNum = new Long(time.getTime()).intValue() / 1000;

            return timeSecondNum;
        } catch (ParseException e) {
            e.printStackTrace();

            return -1;
        }
    }

    /**
     * completed
     * 判断时间是否是工作日
     *
     * @param userDateTimeString 用户日期时间String
     * @return 是需要的时间段返回true
     */
    public static boolean judgeTimeValid(String userDateTimeString) {

        //判断时间是否有效        对于一天来说，共有<=5个休息地原始记录分别对应0:00, 1:00, 2:00, 3:00, 4:00      共有<=5个工作地原始记录分别对应10:00, 11:00, 14:00, 15:00, 16:00
        //这次mapreduce任务只做工作日的
        String userDate = new String(userDateTimeString.substring(0, 10));
        boolean isValidTime = false;

        try {
            Date userDateTime = dateFormat.parse(userDate);
            String week = weekFormat.format(userDateTime);

            System.out.println("WEEK:" + week);
            int userTime = getUserTime(userDateTimeString);
            //增加工作地时间范围，之前从10:00开始
            if (!((week.equals("Sun") && userTime < TIME_20_00) || week.equals("Sat") || (week.equals("Fri") && userTime > TIME_16_OO))) {
                if ((userTime <= TIME_04_00) || (userTime >= TIME_08_50 && userTime <= TIME_16_OO) || (userTime >= TIME_20_00)) {
                    isValidTime = true;
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return isValidTime;
    }

    /**
     *
     * @param currentDate
     * @return
     */
    public static boolean isWeekend(String currentDate) {
        //判断是否周末
        boolean is = false;
        try {
            Date date = dateFormat.parse(currentDate);
            String week = weekFormat.format(date);

            System.out.println("WEEK:" + week);

            switch (week.charAt(2)) {
                case 'a'://周六Sat
                case 'u'://周日Sun
                    is = true;
                    break;
                default:
                    break;
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return is;
    }

    /**
     * 得到算法要求的五个点
     * @param coordinates 已经按时间排好顺序，所有时间点的List
     * @return time, longitude, latitude|time,longitude,latitude|......
     */
    public static String getFivePoints(List<Coordinate> coordinates, int placeFlag) {

        int[] fivePoints = {-1, -1, -1, -1, -1};

        switch (placeFlag) {
            case HOME:
                for (int i = coordinates.size() - 1; i >= 0; i--) {
                    if (-1 == fivePoints[4] && coordinates.get(i).time <= TIME_04_00) {
                        fivePoints[4] = i;
                    }
                    if (-1 == fivePoints[3] && coordinates.get(i).time <= TIME_03_00) {
                        fivePoints[3] = i;
                    }
                    if (-1 == fivePoints[2] && coordinates.get(i).time <= TIME_02_00) {
                        fivePoints[2] = i;
                    }
                    if (-1 == fivePoints[1] && coordinates.get(i).time <= TIME_01_00) {
                        fivePoints[1] = i;
                    }
                    if (-1 == fivePoints[0] && coordinates.get(i).time <= TIME_00_00) {
                        fivePoints[0] = i;
                    }
                }
                break;
            case WORK:
                for (int i = coordinates.size() - 1; i >= 0; i--) {
                    if (-1 == fivePoints[4] && coordinates.get(i).time <= TIME_16_OO) {
                        fivePoints[4] = i;
                    }
                    if (-1 == fivePoints[3] && coordinates.get(i).time <= TIME_15_00) {
                        fivePoints[3] = i;
                    }
                    if (-1 == fivePoints[2] && coordinates.get(i).time <= TIME_14_00) {
                        fivePoints[2] = i;
                    }
                    if (-1 == fivePoints[1] && coordinates.get(i).time <= TIME_11_00) {
                        fivePoints[1] = i;
                    }
                    if (-1 == fivePoints[0] && coordinates.get(i).time <= TIME_10_00) {
                        fivePoints[0] = i;
                    }
                }
                break;
        }

        StringBuffer validCoordinates = new StringBuffer();
        for (int i : fivePoints) {
            if (-1 != i) {
                Coordinate tmp = coordinates.get(i);
                String str = tmp.time + "," + tmp.getLon() + "," + tmp.getLat();
                validCoordinates.append(str);
                validCoordinates.append("|");
            }
        }
        if (validCoordinates.length() > 0) {
            validCoordinates.deleteCharAt(validCoordinates.length() - 1);
        }

        return validCoordinates.toString();


    }
}