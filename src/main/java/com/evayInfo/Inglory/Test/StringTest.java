package com.evayInfo.Inglory.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sunlu on 17/9/1.
 */
public class StringTest {
    public static void main(String[] args) {
        String str = "A;B;C;D";
        String[] arr = str.split(";");
        List<String> list = new ArrayList<String>();
        for (int i = 0; i < arr.length; i++) {
            for (int j = i + 1; j < arr.length; j++) {
                list.add(arr[i] + "," + arr[j]);
            }
        }
        for (int i = 0; i < list.size(); i++) {
            System.out.println(list.get(i));
        }
        ;
    }
}
