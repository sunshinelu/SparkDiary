package com.evayInfo.Inglory.ScalaDiary.DateProcess

import java.util.Date

/**
 * Created by sunlu on 17/10/23.
 * 计算时间间隔，参考链接
 * Scala日期处理
 * http://www.cnblogs.com/wwxbi/p/6116560.html
 */
object TimeIntervalDemo1 {
  def main(args: Array[String]) {

    val d = new java.text.SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new java.util.Date())

    val dateFormat = new java.text.SimpleDateFormat("yyyyMMdd HH:mm:ss")

    // 系统时间
    val d1 = new java.util.Date()
    val nowDate: String = dateFormat.format(d1)

    // 输入指定时间
    val dd: Date = dateFormat.parse("20161229 14:20:50")

    // 时间差
    val d3 = new java.util.Date()
    val d4 = new java.util.Date()
    val diff = d4.getTime - d3.getTime // 返回自此Date对象表示的1970年1月1日，00:00:00 GMT以来的毫秒数。
    val diffMinutes = diff / (1000 * 60) // 时间间隔，单位：分钟
    println("时间间隔(单位：分钟): " + diffMinutes)

  }
}
