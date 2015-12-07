package com.huoteng.mapreduce;

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
    static public final int WEEKEND = 3;
    static public final int WEEKDAY = 2;
    static public final int NO_NEED = 1;
    static public final int HOME = 4;
    static public final int WORK = 5;

    static public final int TIME_00_00 = 86400;
    static public final int TIME_01_00 = 3600;
    static public final int TIME_02_00 = 7200;
    static public final int TIME_03_00 = 10800;
    static public final int TIME_04_00 = 14400;
    static public final int TIME_08_50 = 31800;
    static public final int TIME_10_00 = 36000;
    static public final int TIME_11_00 = 39600;
    static public final int TIME_13_00 = 46800;
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
//            int isValidTime = 0;
//
//            if (week.equals("Sun") || week.equals("Sat")) {
//                isValidTime = UserStatus.NO_NEED;
//            } else if (userTime.after(homeStartTime) && userTime.before(homeEndTime)) {
//                isValidTime = UserStatus.HOME_TIME;
//            } else if ((userTime.after(workStartTime) && userTime.before(workEndTime))) {
//                isValidTime = UserStatus.WORK_TIME;
//            }
//
//            return isValidTime;
//        } catch (ParseException e) {
//            e.printStackTrace();
//            return 0;
//        }
//    }

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
                isValidTime.append(Integer.toString(HOME));
            } else if (time.after(time_08_50) && time.before(time_16_00)) {
                isValidTime.append(Integer.toString(WORK));
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return isValidTime.toString();
    }

    /** completed
     * 将时间转换为当天秒数返回
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

    /** completed
     * 判断时间是否是工作日
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

    /** completed
     * 根据要求得到在家的时间点
     * @param homeTimeCoordinates 已经按时间排好顺序，所有时间点的List
     * @return time,longitude,latitude|time,longitude,latitude|......
     */
    public static String getHomeTimePoint(List<Coordinate> homeTimeCoordinates, boolean isReduce) {

        int[] fivePoints = {-1, -1, -1, -1, -1};

        // 倒序遍历list，找到离0点最近的作为0点、1点、2点、3点、4点可能的位置
        for (int i = homeTimeCoordinates.size() - 1; i >= 0 ; i--) {
            int currentTime = homeTimeCoordinates.get(i).time;
            if((-1 == fivePoints[0]) && (currentTime > TIME_20_00)) {
                fivePoints[0] = i;
                fivePoints[1] = i;
                fivePoints[2] = i;
                fivePoints[3] = i;
                fivePoints[4] = i;
            }
        }

        // 正序遍历list，找到最接近1点、2点、3点、4点可能的位置
        for(int i = 0; i < homeTimeCoordinates.size(); i++) {
            int currentTime = homeTimeCoordinates.get(i).time;
            if ((currentTime <= TIME_01_00) && (currentTime > 0)) {
                fivePoints[1] = i;
            }
            if ((currentTime <= TIME_02_00) && (currentTime > TIME_01_00)) {
                fivePoints[2] = i;
            }
            if ((currentTime <= TIME_03_00) && (currentTime > TIME_02_00)) {
                fivePoints[3] = i;
            }
            if ((currentTime <= TIME_04_00) && (currentTime > TIME_03_00)) {
                fivePoints[4] = i;
            }
        }

        if (isReduce) {
            for (int i = 0; i < 5; i++) {
                if (-1 == fivePoints[i]) {
                    for (int j = i; j >= 0 ; j--) {
                        if (-1 != fivePoints[j]) {
                            fivePoints[i] = fivePoints[j];
                            break;
                        }
                    }
                }
            }
        }

        StringBuffer isValidTime = new StringBuffer();
        for (int i = 0; i < 5; i++) {
            if (-1 != fivePoints[i]) {
                Coordinate validCoordinate = homeTimeCoordinates.get(fivePoints[i]);
                String tmp = validCoordinate.time + "," + validCoordinate.getLon() + "," + validCoordinate.getLat();
                isValidTime.append(tmp);
                isValidTime.append("|");
            }
        }
        if (isValidTime.length() > 0) {
            isValidTime.deleteCharAt(isValidTime.length()-1);
        }

        return isValidTime.toString();

    }




    /** completed
     * 根据要求得到在工作地的时间点
     * @param workTimeCoordinates 已经按时间排好顺序，所有时间点的List
     * @return time,longitude,latitude|time,longitude,latitude|......
     */
    public static String getWorkTimePoint(List<Coordinate> workTimeCoordinates, boolean isReduce) {
        int[] fivePoints = {-1, -1, -1, -1, -1};

        // 正序遍历list，找到最接近10点、11点、14点、15点、16点的位置
        for (int i = 0; i < workTimeCoordinates.size(); i++) {
            int currentTime = workTimeCoordinates.get(i).time;

            if((currentTime <= TIME_10_00) && (currentTime > TIME_08_50)) {
                fivePoints[0] = i;
            }
            if((currentTime <= TIME_11_00) && (currentTime > TIME_08_50)) {
                fivePoints[1] = i;
            }
            if((currentTime <= TIME_14_00) && (currentTime > TIME_08_50)) {
                fivePoints[2] = i;
            }
            if((currentTime <= TIME_15_00) && (currentTime > TIME_08_50)) {
                fivePoints[3] = i;
            }
            if((currentTime <= TIME_16_OO) && (currentTime > TIME_08_50)) {
                fivePoints[4] = i;
            }
        }

        if (isReduce) {
            for (int i = 0; i < 5; i++) {
                if (-1 == fivePoints[i]) {
                    for (int j = i; j >= 0 ; j--) {
                        if (-1 != fivePoints[j]) {
                            fivePoints[i] = fivePoints[j];
                            break;
                        }
                    }
                }
            }
        }

        StringBuffer isValidTime = new StringBuffer();
        for (int i = 0; i < 5; i++) {
            if (-1 != fivePoints[i]) {
                Coordinate validCoordinate = workTimeCoordinates.get(fivePoints[i]);
                String tmp = validCoordinate.time + "," + validCoordinate.getLon() + "," + validCoordinate.getLat();
                isValidTime.append(tmp);
                isValidTime.append("|");
            }
        }
        if (isValidTime.length() > 0) {
            isValidTime.deleteCharAt(isValidTime.length()-1);
        }

        return isValidTime.toString();

    }
}
