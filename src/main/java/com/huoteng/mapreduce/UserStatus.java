package com.huoteng.mapreduce;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * Created by teng on 11/5/15.
 */
public class UserStatus {
    static public final int WEEKEND = 3;
    static public final int WEEKDAY = 2;
    static public final int NO_NEED = 1;

    private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static DateFormat weekFormat = new SimpleDateFormat("E");

//    /**
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

    /**
     * completed
     * @param dateString 用户当前日期时间字符串
     * @return 用户当前日期及状态字符串
     */
    public static String judgeUserDate(String dateString) {
        String userDate = dateString.substring(0, 10);
        StringBuilder result = new StringBuilder(userDate + ",");
        /**
         * 需要在这里判断周末与工作日，并返回
         * 返回值样本：yyyy-MM-dd|status
         */
        try {
            Date userDateTime = dateFormat.parse(userDate);
            String week = weekFormat.format(userDateTime);

            System.out.println("WEEK:" + week);
            if (week.equals("Sun") || week.equals("Sat")) {
                result.append(UserStatus.WEEKEND);
            } else {
                result.append(UserStatus.WEEKDAY);
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }

        return result.toString();
    }

    /**
     * completed
     * @param dateString 用户当前日期时间字符串
     * @return 用户当前时间字符串
     */
    public static String getUserTime(String dateString) {
        String userTime = dateString.substring(11, 23);

        return userTime;
    }
}
