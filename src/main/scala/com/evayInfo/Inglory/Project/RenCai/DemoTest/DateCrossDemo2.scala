package com.evayInfo.Inglory.Project.RenCai.DemoTest

import java.text.SimpleDateFormat

import breeze.linalg.{max, min}

/*
计算2个时间段的重叠天数
https://blog.csdn.net/handsomekang/article/details/78937378


def calc_overlap_days(s1, e1, s2, e2):
    latest_start = max(s1, s2)
    earliest_end = min(e1, e2)
    overlap = (earliest_end - latest_start).days + 1
    if overlap < 0:
        overlap = 0
    return overlap


 */
object DateCrossDemo2 {

  def main(args: Array[String]): Unit = {

    def calc_overlap_days(s1:Long, e1:Long, s2:Long, e2:Long):Int={
      val latest_start = max(s1,s2)
      val earliest_end = min(e1, e2)
      var overlap = (earliest_end - latest_start)/(1000*3600*24)

      if (overlap < 0){
        overlap = 0
      }
      return overlap.toInt
    }

    def calc_overlap_days_2(s1:String, e1:String, s2:String, e2:String):Int={
      //定义时间格式
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
      val latest_start = max(dateFormat.parse(s1).getTime,dateFormat.parse(s2).getTime)
      val earliest_end = min(dateFormat.parse(e1).getTime, dateFormat.parse(e2).getTime)
      var overlap = (earliest_end - latest_start)/(1000*3600*24) +1

      if (overlap < 0){
        overlap = 0
      }
      return overlap.toInt
    }

//    val s1 = "2017-10-01"
//    val e1 = "2017-10-03"
//    val s2 = "2017-10-02"
//    val e2 = "2017-10-04"
    val s1 = "2014-10-01"
    val e1 = "2018-10-01"
    val s2 = "2004-10-01"
    val e2 = "2008-10-01"

    val test1= calc_overlap_days_2(s1,e1,s2,e2)
    println(s"时间间隔为：$test1")

    val years = test1 / 365.0
    println(s"时间间隔为：$years 年")

  }

}
