package com.evayInfo.Inglory.util;

// import java.text.DateFormat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Date related utility methods and constants
 */
public class DateUtils {
    /**
     * The UTC time zone. Not sure if {@link TimeZone#getTimeZone(String)}
     * understands "UTC" in all environments, but it'll fall back to GMT
     * in such cases, which is in practice equivalent to UTC.
     */
    public static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    /**
     * Custom time zone used to interpret date values without a time
     * component in a way that most likely falls within the same day
     * regardless of in which time zone it is later interpreted. For
     * example, the "2012-02-17" date would map to "2012-02-17T12:00:00Z"
     * (instead of the default "2012-02-17T00:00:00Z"), which would still
     * map to "2012-02-17" if interpreted in say Pacific time (while the
     * default mapping would result in "2012-02-16" for UTC-8).
     */
    public static final TimeZone MIDDAY = TimeZone.getTimeZone("GMT-12:00");

    /**
     * Returns a ISO 8601 representation of the given date. This method
     * is thread safe and non-blocking.
     *
     * @param date given date
     * @return ISO 8601 date string, including timezone details
     * @see <a href="https://issues.apache.org/jira/browse/TIKA-495">TIKA-495</a>
     */
    public static String formatDate(Date date) {
        Calendar calendar = GregorianCalendar.getInstance(UTC, Locale.US);
        calendar.setTime(date);
        return doFormatDate(calendar);
    }

    /**
     * Returns a ISO 8601 representation of the given date. This method
     * is thread safe and non-blocking.
     *
     * @param date given date
     * @return ISO 8601 date string, including timezone details
     * @see <a href="https://issues.apache.org/jira/browse/TIKA-495">TIKA-495</a>
     */
    public static String formatDate(Calendar date) {
        // Explicitly switch it into UTC before formatting 
        date.setTimeZone(UTC);
        return doFormatDate(date);
    }

    /**
     * Returns a ISO 8601 representation of the given date, which is
     * in an unknown timezone. This method is thread safe and non-blocking.
     *
     * @param date given date
     * @return ISO 8601 date string, without timezone details
     * @see <a href="https://issues.apache.org/jira/browse/TIKA-495">TIKA-495</a>
     */
    public static String formatDateUnknownTimezone(Date date) {
        // Create the Calendar object in the system timezone
        Calendar calendar = GregorianCalendar.getInstance(TimeZone.getDefault(), Locale.US);
        calendar.setTime(date);
        // Have it formatted
        String formatted = formatDate(calendar);
        // Strip the timezone details before returning
        return formatted.substring(0, formatted.length() - 1);
    }

    private static String doFormatDate(Calendar calendar) {
        return String.format(
                Locale.ROOT,
                "%04d-%02d-%02dT%02d:%02d:%02dZ",
                calendar.get(Calendar.YEAR),
                calendar.get(Calendar.MONTH) + 1,
                calendar.get(Calendar.DAY_OF_MONTH),
                calendar.get(Calendar.HOUR_OF_DAY),
                calendar.get(Calendar.MINUTE),
                calendar.get(Calendar.SECOND));
    }

    public static String toCron(String zq, String startTime) {
        String cron = "";
        startTime = startTime.replaceAll("-", "").replaceAll(":", "").replace(" ", "");
        int f = Integer.parseInt(startTime.substring(10, 12));
        int s = Integer.parseInt(startTime.substring(8, 10));
        int r = Integer.parseInt(startTime.substring(6, 8));
        if (zq.equals("0")) {//一次
            cron = "* " + String.valueOf(f) + " " + String.valueOf(s) + " * * ?";
        } else if (zq.equals("1")) {//天
            cron = "* " + String.valueOf(f) + " " + String.valueOf(s) + " * * ?";
        } else if (zq.equals("2")) {//周
            cron = "* " + String.valueOf(f) + " " + String.valueOf(s) + " 0/7 * ?";
        } else if (zq.equals("3")) {//月
            cron = "* " + String.valueOf(f) + " " + String.valueOf(s) + " " + String.valueOf(r) + " * * ?";
        }
        return cron;
    }

    /**
     * @return
     * @Description: 获取当前时间 格式为yyyy-MM-dd HH:mm:ss
     */
    public static String getCurrentDate() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar rightNow = Calendar.getInstance();
        return df.format(rightNow.getTime());
    }

    /**
     * @return
     * @Description: 获取当前时间 格式为yyyy-MM-dd
     */
    public static String getCurrentDate(String str) {
        SimpleDateFormat df = new SimpleDateFormat(str);
        Calendar rightNow = Calendar.getInstance();
        return df.format(rightNow.getTime());
    }

    /**
     * 字符串转换成日期
     *
     * @param str
     * @return date
     */
    public static Date StrToDate(String str) {

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date date = null;
        try {
            date = format.parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }


    /**
     * 字符串转换成日期
     *
     * @param str
     * @return date
     */
    public static Date StrToDate(String str, String formatStr) {

        SimpleDateFormat format = new SimpleDateFormat(formatStr);
        Date date = null;
        try {
            date = format.parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }

    /**
     * 字符串转换成long类型
     *
     * @param str
     * @return date
     */
    public static long StrToLong(String str, String formatStr) {

        SimpleDateFormat format = new SimpleDateFormat(formatStr);
        Date date = null;
        try {
            date = format.parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        //  System.out.println(date.getTime());
        return date.getTime();

    }

    /**
     * 方法名: toCronLinux
     * 描述: 大写的todo linux的定时任务
     * 参数: @param zq
     * 参数: @param startTime
     * 参数: @return
     * 返回类型: String
     * 创建人：孙伟
     * 创建时间：May 15, 2017 11:48:08 AM
     * 修改人：孙伟
     * 修改时间：May 15, 2017 11:48:08 AM
     * 修改备注：
     * 其他:
     */
    public static String toCronLinux(String timeStr) {
        String cron = "";
        Date d = StrToDate(timeStr, "yyyy-MM-dd HH:mm:ss");
        Date d1 = new Date(d.getTime() + 2 * 60 * 1000);
        cron = d1.getMinutes() + " " + d1.getHours() + " * * * ";
        System.out.println(cron);
        return cron;

    }

    public static String longToDate(long lo, String format) {
        Date date = new Date(lo);
        SimpleDateFormat sd = new SimpleDateFormat(format);
        return sd.format(date);
    }


    /**
     * 微博处理时间，今天 07:54 ，5分钟前
     *
     * @param ctime 时间
     *              // @param format
     *              格式 格式描述:例如:yyyy-MM-dd yyyy-MM-dd HH:mm
     * @return
     * @author wxy
     */
    public static String parseTime_weibo(String ctime) {

        String format = "yyyy-MM-dd HH:mm";
        String resulttime = "";
        SimpleDateFormat df = new SimpleDateFormat(format);
        if (ctime.contains("分钟前")) {
            String[] times = ctime.split("分钟前");
            // 如果是分钟前，按如下处理
            String mm = times[0].replaceAll("\\s+", "").trim();
            //String[] times1= mm.split("-");
            long seconds = Long.parseLong(mm);
            long nowtime = System.currentTimeMillis();
            // 当前时间减seconds*60*1000毫秒
            long result = nowtime - seconds * 60 * 1000;
            resulttime = df.format(result);
        } else if (ctime.contains("今天")) {
            String today = getCurrentDate("yyyy-MM-dd");
            ctime = ctime.replace("今天", today);
            resulttime = ctime;
        } else if (ctime.length() == 11) {
            resulttime = getCurrentDate("yyyy-") + ctime;
        } else {
            resulttime = ctime;
        }

        return resulttime;
    }


    public static void main(String[] args) {

        parseTime_weibo("07-18 12:37");

        //toCronLinux("2017-05-13 18:52:00");
        //	long d=StrToLong("2017-05-13 18:52:00","yyyy-MM-dd HH:mm:ss");
        //System.out.println(longToDate(d,"yyyy-MM-dd HH:mm:ss"));
        // DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") ;
        //	String s= format.format(new Date(d.getTime()+2*60*1000));
        //System.out.println(s);

        //System.out.println(d1.getMinutes()+":"+d1.getSeconds());


        //toCron("3","2017-09-12 12:00:00");
    }
}
