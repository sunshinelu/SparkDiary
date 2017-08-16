package com.evayInfo.Inglory.TestCode

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created by sunlu on 17/8/16.
 */
object timeExchangeTest {

  def main(args: Array[String]) {


    val longT1 = "3545793265183241014"
    val longT2 = "1496647712000"


    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd

    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)
    //把时间转换成long类型
    val todayL = dateFormat.parse(today).getTime
    println(todayL)
    println(todayL.toString.length)

    val date: String = dateFormat.format(new Date((todayL * 1000l)))
    println(date)

    /*
    2、由long类型转换成Date类型
     */
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //前面的lSysTime是秒数，先乘1000得到毫秒数，再转为java.util.Date类型
    val dt = new Date(longT2.toLong * 1000)
    val sDateTime = sdf.format(dt) //得到精确到秒的表示：08/31/2006 21:08:00
    println(sDateTime)


  }

}
