package com.evayInfo.Inglory.TestCode


import java.text.SimpleDateFormat
import java.util.Date

import com.evayInfo.Inglory.util.DateUtils

/**
 * Created by sunlu on 17/7/31.
 */
object DateTest {

  def main(args: Array[String]) {

    val s = "3小时前"
    val tt1 = DateUtils.parseTime_weibo(s)
    println(tt1)

    //定义时间格式
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
    val now: Date = new Date()
    val today = dateFormat.format(now)
    println("当前时间为：" + today)
    val todayL = dateFormat.parse(today).getTime
    println("Long类型的当前时间为：" + todayL)

    val l = "1499993438000".toLong
    val l2 = "1503564130000".toLong
    val l3 = "1503889810000".toLong
    val l2String = dateFormat.format(new Date(l3 * 1000L))
    println(l2String)

    println(dateFormat.format(new Date(l3)))

  }

}
