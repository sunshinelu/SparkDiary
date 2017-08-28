package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created by sunlu on 17/8/28.
 * 将Long类型的时间戳转换成时间串
 */
class DateLongToString {
  def main(args: Array[String]) {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val l3 = "1503889810000".toLong
    println(dateFormat.format(new Date(l3)))
  }
}
