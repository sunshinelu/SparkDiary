package com.evayInfo.Inglory.SparkDiary.database.solr;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Date related utility methods and constants
 */
public class DateUtils {
    /**
     * The UTC time zone. Not sure if {@link TimeZone#getTimeZone(String)}
     * understands "UTC" in all environments, but it'll fall back to GMT in such
     * cases, which is in practice equivalent to UTC.
     */
    public static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    /**
     * Custom time zone used to interpret date values without a time component
     * in a way that most likely falls within the same day regardless of in
     * which time zone it is later interpreted. For example, the "2012-02-17"
     * date would map to "2012-02-17T12:00:00Z" (instead of the default
     * "2012-02-17T00:00:00Z"), which would still map to "2012-02-17" if
     * interpreted in say Pacific time (while the default mapping would result
     * in "2012-02-16" for UTC-8).
     */
    public static final TimeZone MIDDAY = TimeZone.getTimeZone("GMT-12:00");

    /**
     * Returns a ISO 8601 representation of the given date. This method is
     * thread safe and non-blocking.
     *
     * @param date given date
     * @return ISO 8601 date string, including timezone details
     * @see <a
     * href="https://issues.apache.org/jira/browse/TIKA-495">TIKA-495</a>
     */
    public static String formatDate(Date date) {
        Calendar calendar = GregorianCalendar.getInstance(UTC, Locale.US);
        calendar.setTime(date);
        return doFormatDate(calendar);
    }

    /**
     * Returns a ISO 8601 representation of the given date. This method is
     * thread safe and non-blocking.
     *
     * @param date given date
     * @return ISO 8601 date string, including timezone details
     * @see <a
     * href="https://issues.apache.org/jira/browse/TIKA-495">TIKA-495</a>
     */
    public static String formatDate(Calendar date) {
        // Explicitly switch it into UTC before formatting
        date.setTimeZone(UTC);
        return doFormatDate(date);
    }

    /**
     * Returns a ISO 8601 representation of the given date, which is in an
     * unknown timezone. This method is thread safe and non-blocking.
     *
     * @param date given date
     * @return ISO 8601 date string, without timezone details
     * @see <a
     * href="https://issues.apache.org/jira/browse/TIKA-495">TIKA-495</a>
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
        return String.format(Locale.ROOT, "%04d-%02d-%02dT%02d:%02d:%02dZ", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1,
                calendar.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND));
    }

    public static String toCron(String zq, String startTime) {
        String cron = "";
        startTime = startTime.replaceAll("-", "").replaceAll(":", "").replace(" ", "");
        int f = Integer.parseInt(startTime.substring(10, 12));
        int s = Integer.parseInt(startTime.substring(8, 10));
        int r = Integer.parseInt(startTime.substring(6, 8));
        if (zq.equals("0")) {// 一次
            cron = "* " + String.valueOf(f) + " " + String.valueOf(s) + " * * ?";
        } else if (zq.equals("1")) {// 天
            cron = "* " + String.valueOf(f) + " " + String.valueOf(s) + " * * ?";
        } else if (zq.equals("2")) {// 周
            cron = "* " + String.valueOf(f) + " " + String.valueOf(s) + " 0/7 * ?";
        } else if (zq.equals("3")) {// 月
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
        // System.out.println(date.getTime());
        return date.getTime();

    }

    /**
     * 方法名: toCronLinux 描述: TODO linux的定时任务 参数: @param zq 参数: @param startTime
     * 参数: @return 返回类型: String 创建人：孙伟 创建时间：May 15, 2017 11:48:08 AM 修改人：孙伟
     * 修改时间：May 15, 2017 11:48:08 AM 修改备注： 其他:
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
     * 微博处理时间，今天 07:54 ，5分钟前、刚刚、4小时前
     *
     * @param ctime  时间
     * @param format 格式 格式描述:例如:yyyy-MM-dd yyyy-MM-dd HH:mm
     * @return
     * @author wxy
     */
    public static String parseTime_weibo(String ctime) {
        String resulttime = "";
        String format = "yyyy-MM-dd HH:mm:ss";
        SimpleDateFormat df = new SimpleDateFormat(format);
        if (ctime.contains("分钟前")) {
            String[] times = ctime.split("分钟前");
            // 如果是分钟前，按如下处理
            String mm = times[0].replaceAll("\\s+", "").trim();
            // String[] times1= mm.split("-");
            long seconds = Long.parseLong(mm);
            long nowtime = System.currentTimeMillis();
            // 当前时间减seconds*60*1000毫秒
            long result = nowtime - seconds * 60 * 1000;
            resulttime = df.format(result);
        } else if (ctime.contains("刚刚")) { // 当做1分钟前处理

            // 如果是分钟前，按如下处理
            String mm = "1";
            long seconds = Long.parseLong(mm);
            long nowtime = System.currentTimeMillis();
            // 当前时间减seconds*60*1000毫秒
            long result = nowtime - seconds * 60 * 1000;
            resulttime = df.format(result);
        } else if (ctime.contains("小时前")) {
            String[] times = ctime.split("小时前");
            // 如果是分钟前，按如下处理
            String hour = times[0].replaceAll("\\s+", "").trim();
            // String[] times1= mm.split("-");
            long hourL = Long.parseLong(hour);
            long nowtime = System.currentTimeMillis();
            // 当前时间减seconds*60*1000毫秒
            long result = nowtime - hourL * 60 * 60 * 1000;
            resulttime = df.format(result);
        } else if (ctime.contains("今天")) {
            String today = getCurrentDate("yyyy-MM-dd");
            ctime = ctime.replace("今天", today);
            resulttime = ctime;
        } else if (ctime.length() == 11) {
            resulttime = getCurrentDate("yyyy-") + ctime;
        } else if (ctime.length() == 5 && ctime.contains("-")) {
            resulttime = getCurrentDate("yyyy-") + ctime + " 00:00:00";
        } else if (ctime.contains("昨天")) {

            // 如果是分钟前，按如下处理
            String hour = "24";
            // String[] times1= mm.split("-");
            long hourL = Long.parseLong(hour);
            long nowtime = System.currentTimeMillis();
            // 当前时间减seconds*60*1000毫秒
            long result = nowtime - hourL * 60 * 60 * 1000;
            resulttime = df.format(result);
        } else {
            resulttime = ctime;
        }
        if (resulttime.length() == 10) {
            resulttime = resulttime + " 00:00:00";
        }
        return resulttime;
    }

    /**
     * 字符串转换成格式化
     *
     * @param str
     * @return date
     */
    public static String StrToStr(String str, String formatStr) {
        String temp = "";
        SimpleDateFormat format = new SimpleDateFormat(formatStr);
        Date date = null;
        try {
            date = format.parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        temp = longToDate(date.getTime(), formatStr);
        // System.out.println(temp);
        return temp;
    }

    /**
     * @param @param  s
     * @param @return 设定文件
     * @return String 返回类型
     * @throws
     * @Title: getDate
     * @Description: TODO(解析时间)
     */
    public static String getDate(String s) {
        String reTime = s.toString().replace("amp;", "");
        String format = "yyyy-MM-dd HH:mm:ss";
        SimpleDateFormat df = new SimpleDateFormat(format);
        if (reTime.contains("分钟前")) {
            String[] times = reTime.split("分钟前");
            // 如果是分钟前，按如下处理
            String mm = times[0].replace("\\s+", "").trim();
            long seconds = Long.parseLong(mm);
            long nowtime = System.currentTimeMillis();
            // 当前时间减seconds*60*1000毫秒
            long result = nowtime - seconds * 60 * 1000;
            reTime = df.format(result);
        } else if (reTime.contains("小时前")) {
            String[] times = reTime.split("小时前");
            // 如果是分钟前，按如下处理
            String mm = times[0].replace("\\s+", "").trim();
            long seconds = Long.parseLong(mm);
            long nowtime = System.currentTimeMillis();
            // 当前时间减seconds*60*1000毫秒
            long result = nowtime - seconds * 60 * 1000;
            reTime = df.format(result);
        } else {
            if (reTime.contains("今")) {
                String times = reTime.split("今")[1].replace("\\s+", "").trim();
                // 如果是小时前，按如下处理
                reTime = df.format(new Date());
            } else if (reTime.contains("昨")) {
                // 否则，不视为今的数据，统一处理成24小时前的时间，按如下处理
                long nowtime = System.currentTimeMillis();
                // 当前时间减seconds*60*1000毫秒
                long result = nowtime - 24 * 60 * 60 * 1000;
                reTime = df.format(result);
                // System.out.println("解析的时间："+reTime);
            } else {
                reTime = findTimeInUrl(reTime);
            }
        }
        return reTime;
    }

    /**
     * @param @param  content
     * @param @return 设定文件
     * @return String 返回类型
     * @throws
     * @Title: findTimeInUrl
     * @Description: TODO(根據內容和獲取時間)
     */
    public static String findTimeInUrl(String content) {
        // System.out.println("this is time:"+content);
        String regex1 = "\\d{4}(\\-|\\/|.)\\d{1,2}\\1\\d{1,2}"; // 处理2017/7/7
        // 14:23:40 的样式
        Pattern p1 = Pattern.compile(regex1);
        Matcher m1 = p1.matcher(content);
        if (m1.find()) {
            content = m1.group().toString();
        }
        String result = "";
        String regex = "(([1-9]\\d{3})(-|/|.| |\\s{1,})(0\\d{1}|1[0-2])(-|/|.| |\\s{1,})(0\\d{1}|[12]\\d{1}|3[01]))|((19|20)\\d{2}(((0[13578]|1[02])([0-2]\\d|3[01]))|((0[469]|11)([0-2]\\d|30))|(02([01]\\d|2[0-8]))))|((19|20)\\d{2}/?\\d{1,2}/\\d{1,2})";
        Pattern p = Pattern.compile(regex);
        content = content.replaceAll("年", "-").replaceAll("月", "-").replaceAll("日", "");
        content = content.length() == 4 ? "0" + content : content;
        content = content.replaceAll(":", "").replaceAll(": ", "");
        content = content.replaceAll(" ", "");
        content = content.replaceAll("\t", "-");
        content = content.replaceAll("\\ ", "");
        content = content.replaceAll("\\s*", "");
        content = content.replace("[", "").replace("]", "").replace("(", "").replace(")", "");
        content = content.trim();
        Matcher m = p.matcher(content);
        if (m.find()) {
            if (content.length() == 5) {
                result = Calendar.getInstance().get(Calendar.YEAR) + "-" + content;
            }
            if (result.length() == 8 && (!result.contains("-"))) {
                result = result.substring(0, 4) + "-" + result.substring(4, 6) + "-" + result.substring(6, 8);
            }
            result = m.group().toString();
            result = result.replace("/", "-").replace("_", "-").replace(".", "-").replace("\\", "-");
        } else {
            regex = "(([1-9]\\d{1})(-|/|.| |\\s{1,})(0\\d{1}|1[0-2])(-|/|.| |\\s{1,})(0\\d{1}|[12]\\d{1}|3[01]))|((19|20)\\d{2}(((0[13578]|1[02])([0-2]\\d|3[01]))|((0[469]|11)([0-2]\\d|30))|(02([01]\\d|2[0-8]))))|((19|20)\\d{2}/?\\d{1,2}/\\d{1,2})";
            p = Pattern.compile(regex);
            m = p.matcher(content);
            if (m.find()) {
                result = m.group().toString();
                result = "20" + result.replace("/", "-").replace("_", "-").replace(".", "-");
            } else {
                regex = "(([1-9]\\d{3})(-|/|.| |\\s{1,})(\\d{1,}|1[0-2])(-|/|.| |\\s{1,})(\\d{1,}|[12]\\d{1,}|3[01]))|((19|20)\\d{2}(((0[13578]|1[02])([0-2]\\d|3[01]))|((0[469]|11)([0-2]\\d|30))|(02([01]\\d|2[0-8]))))|((19|20)\\d{2}/?\\d{1,2}/\\d{1,2})";
                p = Pattern.compile(regex);
                m = p.matcher(content);
                if (m.find()) {
                    result = m.group().toString();
                    result = result.replace("/", "-").replace("_", "-").replace(".", "-");
                } else if (content.length() == 5) {
                    result = Calendar.getInstance().get(Calendar.YEAR) + "-" + content;
                }
            }
        }
        if (!result.equals("")) {
            result = DateUtils.StrToStr(result, "yyyy-mm-dd"); // 处理2017-7-23
            // 的结构，转为2017-07-23
        }
        return result;
    }

    /**
     * 解析时间，如4分钟前、2小时前等
     *
     * @param ctime  时间
     * @param format 格式 格式描述:例如:yyyy-MM-dd yyyy-MM-dd HH:mm:ss
     * @return
     * @author wxy
     */
    public static String parseTime(String ctime) {
        String format = "yyyy-MM-dd HH:mm:ss";
        String resulttime = "";
        if (ctime == null || ctime.equals(""))
            return resulttime;
        SimpleDateFormat df = new SimpleDateFormat(format);

        if (ctime.contains("分钟前")) {
            String[] times = ctime.split("分钟前");
            // 如果是分钟前，按如下处理
            String mm = times[0].replaceAll("\\s+", "").trim();
            long seconds = Long.parseLong(mm);
            long nowtime = System.currentTimeMillis();
            // 当前时间减seconds*60*1000毫秒
            long result = nowtime - seconds * 60 * 1000;
            resulttime = df.format(result);
        } else if (ctime.contains("小时前")) {
            String[] times = ctime.split("小时前");
            // 如果是分钟前，按如下处理
            String mm = times[0].replaceAll("\\s+", "").trim();
            long seconds = Long.parseLong(mm);
            long nowtime = System.currentTimeMillis();
            // 当前时间减seconds*60*1000毫秒
            long result = nowtime - seconds * 60 * 1000;
            resulttime = df.format(result);
        } else {
            if (ctime.contains("今")) {
                String[] times = ctime.split("今");
                // 如果是小时前，按如下处理
                resulttime = df.format(new Date());
            } else {
                // 否则，不视为今的数据，统一处理成24小时前的时间，按如下处理
                long nowtime = System.currentTimeMillis();
                // 当前时间减seconds*60*1000毫秒
                long result = nowtime - 24 * 60 * 60 * 1000;
                resulttime = df.format(result);
            }
        }
        // System.out.println(resulttime);
        return resulttime;
    }

    public static void main(String[] args) {
        // StrToStr("\x00\x00\x01]X\xC8\xC1P","yyyy-mm-dd");
        System.out.println(longToDate(1503666336592l, "yyyy-MM-dd HH:mm:ss")); //StrToLong("1503839841864", "yyyy-MM-dd HH:mm:ss");
//		parseTime_weibo("7月20日 21:44");
//		String ss = "1234";
//		System.out.println(ss.substring(1, ss.length() - 1));
        // toCronLinux("2017-05-13 18:52:00");
        // long d=StrToLong("2017-05-13 18:52:00","yyyy-MM-dd HH:mm:ss");
        // System.out.println(longToDate(d,"yyyy-MM-dd HH:mm:ss"));
        // DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") ;
        // String s= format.format(new Date(d.getTime()+2*60*1000));
        // System.out.println(s);

        // System.out.println(d1.getMinutes()+":"+d1.getSeconds());

        // toCron("3","2017-09-12 12:00:00");
    }
}