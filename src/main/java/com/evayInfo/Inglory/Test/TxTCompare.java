package com.evayInfo.Inglory.Test;

import java.util.Arrays;

/**
 * Created by sunlu on 18/7/19.
 *
 * 将两个字符串找出不同，并将不同处高亮显示
 * https://blog.csdn.net/baidu_38592501/article/details/78537504
 */
public class TxTCompare {
    public static String[] getHighLightDifferent(String a, String b) {
        String[] temp = getDiff(a, b);
        String[] result = {getHighLight(a, temp[0]), getHighLight(b, temp[1])};
        return result;
    }

    private static String getHighLight(String source, String temp) {
        StringBuffer sb = new StringBuffer();
        char[] sourceChars = source.toCharArray();
        char[] tempChars = temp.toCharArray();
        boolean flag = false;
        for (int i = 0; i < sourceChars.length; i++) {
            if (tempChars[i] != ' ') {
                if (i == 0) sb.append("<span style='color:blue'>").append(sourceChars[i]);
                else if (flag) sb.append(sourceChars[i]);
                else sb.append("<span style='color:blue'>").append(sourceChars[i]);
                flag = true;
                if (i == sourceChars.length - 1) sb.append("</span>");
            } else if (flag == true) {
                sb.append("</span>").append(sourceChars[i]);
                flag = false;
            } else sb.append(sourceChars[i]);
        }
        return sb.toString();
    }

    public static String[] getDiff(String a, String b) {
        String[] result = null;
        //选取长度较小的字符串用来穷举子串
        if (a.length() < b.length()) {
            result = getDiff(a, b, 0, a.length());
        } else {
            result = getDiff(b, a, 0, b.length());
            result = new String[]{result[1], result[0]};
        }
        return result;
    }

    //将a的指定部分与b进行比较生成比对结果
    private static String[] getDiff(String a, String b, int start, int end) {
        String[] result = new String[]{a, b};
        int len = result[0].length();
        while (len > 0) {
            for (int i = start; i < end - len + 1; i++) {
                String sub = result[0].substring(i, i + len);
                int idx = -1;
                if ((idx = result[1].indexOf(sub)) != -1) {
                    result[0] = setEmpty(result[0], i, i + len);
                    result[1] = setEmpty(result[1], idx, idx + len);
                    if (i > 0) {
                        //递归获取空白区域左边差异
                        result = getDiff(result[0], result[1], 0, i);
                    }
                    if (i + len < end) {
                        //递归获取空白区域右边差异
                        result = getDiff(result[0], result[1], i + len, end);
                    }
                    len = 0;//退出while循环
                    break;
                }
            }
            len = len / 2;
        }
        return result;
    }

    //将字符串s指定的区域设置成空格
    public static String setEmpty(String s, int start, int end) {
        char[] array = s.toCharArray();
        for (int i = start; i < end; i++) {
            array[i] = ' ';
        }
        return new String(array);
    }


    public static void main(String[] args) {

        String a = "中华人民共和国";
        String b = "中华人名共和国";
        String[] result = getHighLightDifferent(a,b);
        System.out.println(result);
        System.out.println(Arrays.toString(result));


    }
}
