package com.evayInfo.Inglory.ScalaDiary.DateProcess

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.collection.mutable.ListBuffer


/**
 * Created by sunlu on 17/10/23.
 * 生成日期序列，参考链接：
 * Scala日期处理
 * http://www.cnblogs.com/wwxbi/p/6116560.html
 */
object DateSequenceDemo1 {
  def main(args: Array[String]) {
    // 输入开始日期和结束日期
    val stringDateBegin: String = "20160101"
    val stringDateEnd: String = "20160209"

    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val dateBegin: Date = dateFormat.parse(stringDateBegin)
    val dateEnd: Date = dateFormat.parse(stringDateEnd)

    val calendarBegin: Calendar = Calendar.getInstance()
    val calendarEnd: Calendar = Calendar.getInstance()

    calendarBegin.setTime(dateBegin)
    calendarEnd.setTime(dateEnd)

    // 计算日期间隔天数
    val diff = calendarEnd.getTimeInMillis() - calendarBegin.getTimeInMillis()
    val diffDay = (diff / (1000 * 60 * 60 * 24)).toInt
    val calendarList = new ListBuffer[String]()
    for (d <- 0 to diffDay) {
      // 日期转化成"yyyyMMdd"
      calendarList.append(dateFormat.format(calendarBegin.getTime()))
      calendarBegin.add(Calendar.DAY_OF_MONTH, 1)
    }

    println(calendarList.mkString(","))
    calendarList.foreach(println)
  }

}
