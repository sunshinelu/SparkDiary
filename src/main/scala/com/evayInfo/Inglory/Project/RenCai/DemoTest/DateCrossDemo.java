package com.evayInfo.Inglory.Project.RenCai.DemoTest;


import java.text.SimpleDateFormat;
import java.util.*;

/*

https://blog.csdn.net/tgyman/article/details/78616771
 */

public class DateCrossDemo {
    /**
     * 判断两个时间区间是否有交集的方法
     * 
     * @param date1_1
     *            区间1的时间始
     * @param date1_2
     *            区间1的时间止
     * @param date2_1
     *            区间2的时间始
     * @param date2_2
     *            区间2的时间止
     * @return 区间1和区间2如果存在交集,则返回true,否则返回falses
     */
    public static boolean isDateCross(Date date1_1, Date date1_2, Date date2_1, Date date2_2) throws Exception{
        boolean flag = false;// 默认无交集
        long l1_1 = date1_1.getTime();
        long l1_2 = date1_2.getTime();
        long l2_1 = date2_1.getTime();
        long l2_2 = date2_2.getTime();

        if((l1_1>l1_2)||(l2_1>l2_2))
            throw new Exception("Parameter error:date1_1 should not be great than date1_2 and date2_1 should not be great than date2_2");
        if (((l1_1 <= l2_1) && (l2_1 <= l1_2)) || ((l1_1 <= l2_2) && (l2_2 <= l1_2))
                || ((l2_1 <= l1_1) && (l1_1 <= l2_2)) || ((l2_1 <= l1_2) && (l1_2 <= l2_2))) {
            flag = true;
        }
        return flag;
    }


    public static void main(String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String dateStr1_1 = "2017-10-01";
        String dateStr1_2 = "2017-10-03";
        String dateStr2_1 = "2017-10-02";
        String dateStr2_2 = "2017-10-04";
        Date date1_1 = sdf.parse(dateStr1_1);
        Date date1_2 = sdf.parse(dateStr1_2);
        Date date2_1 = sdf.parse(dateStr2_1);
        Date date2_2 = sdf.parse(dateStr2_2);
        boolean b = isDateCross(date1_1, date1_2, date2_1, date2_2);
        System.out.println(b == true ? "有交集" : "无交集");
    }
}

