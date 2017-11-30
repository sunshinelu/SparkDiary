package com.evayInfo.Inglory.Test;

/**
 * Created by sunlu on 17/11/30.
 * 测试电脑性能
 */
public class Test02 {
    public static void main(String[] args) {
        long start=System.currentTimeMillis();

        for(int i=0;i<1000000;i++){
            System.out.println(i);
        }
        long end=System.currentTimeMillis();
        System.out.println("_____________"+(end-start));
    }
}
