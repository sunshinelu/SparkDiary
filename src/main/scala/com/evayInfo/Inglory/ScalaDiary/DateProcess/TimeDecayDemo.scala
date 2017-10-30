package com.evayInfo.Inglory.ScalaDiary.DateProcess

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import breeze.numerics.log

/**
 * Created by sunlu on 17/10/30.
 * 计算时间衰减因子：
 * 1.0 ／ (log(t - prev_t) ＋ 1)
 *
 * t: tomorrow time
 * prev_t: privious time
 *
 */
object TimeDecayDemo {

  //获取今天日期
  def getNowDate(): Long = {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var today = dateFormat.format(now)
    val todayL = dateFormat.parse(today).getTime
    todayL
  }

  //获取明天日期
  def getTomorrow(): Long = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, 1)
    var tomorrow = dateFormat.format(cal.getTime())
    val tomorrowL = dateFormat.parse(tomorrow).getTime
    tomorrowL
  }

  //获取昨天日期
  def getYesterday(): Long = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    val yesterdayL = dateFormat.parse(yesterday).getTime
    yesterdayL
  }

  //获取前天日期
  def get3Dasys(): Long = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -2)
    var threeDays = dateFormat.format(cal.getTime())
    val threeDaysL = dateFormat.parse(threeDays).getTime
    threeDaysL
  }

  //获取一周前日期
  def get7Dasys(): Long = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -7)
    var sevenDays = dateFormat.format(cal.getTime())
    val sevenDaysL = dateFormat.parse(sevenDays).getTime
    sevenDaysL
  }

  //获取半月前日期
  def getHalfMonth(): Long = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -15)
    var halfMonth = dateFormat.format(cal.getTime())
    val halfMonthL = dateFormat.parse(halfMonth).getTime
    halfMonthL
  }
  //获取一月前日期
  def getOneMonth(): Long = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.MONTH, -1)
    var oneMonth = dateFormat.format(cal.getTime())
    val oneMonthL = dateFormat.parse(oneMonth).getTime
    oneMonthL
  }

  //获取六月前日期
  def getSixMonth(): Long = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.MONTH, -6)
    var sixMonth = dateFormat.format(cal.getTime())
    val siMonthL = dateFormat.parse(sixMonth).getTime
    siMonthL
  }

  //获取一年前日期
  def getOneYear(): Long = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.YEAR, -1)
    var oneYear = dateFormat.format(cal.getTime())
    val oneYearL = dateFormat.parse(oneYear).getTime
    oneYearL
  }


  def main(args: Array[String]) {

    val tomorrow = getTomorrow()
    val today = getNowDate()
    val yesterday = getYesterday()
    val threeDaysAgo = get3Dasys()
    val sevenDaysAgo = get7Dasys()
    val halfMonthAgo = getHalfMonth()
    val oneMonthAgo = getOneMonth()
    val sixMonthAgo = getSixMonth()
    val oneYearAgo = getOneYear()

    println(((tomorrow - today).toDouble / (1000 * 60 * 60 * 24)))//.toDouble)
    println(log(((tomorrow - today).toDouble / (1000 * 60 * 60 * 24))))

    val timeDecay_1 = 1.0 / (log(((tomorrow - today).toDouble / (1000 * 60 * 60 * 24))) + 1)
    println("今天的衰减因子为：" + timeDecay_1)


    val timeDecay_2 = 1.0 / (log(((tomorrow - yesterday).toDouble / (1000 * 60 * 60 * 24))) + 1)
    println("昨天的衰减因子为：" + timeDecay_2)

    val timeDecay_3 = 1.0 / (log(((tomorrow - threeDaysAgo).toDouble / (1000 * 60 * 60 * 24))) + 1)
    println("前天的衰减因子为：" + timeDecay_3)

    val timeDecay_4 = 1.0 / (log(((tomorrow - sevenDaysAgo).toDouble / (1000 * 60 * 60 * 24))) + 1)
    println("一周前的衰减因子为：" + timeDecay_4)

    val timeDecay_5 = 1.0 / (log(((tomorrow - halfMonthAgo).toDouble / (1000 * 60 * 60 * 24))) + 1)
    println("半月前的衰减因子为：" + timeDecay_5)

    val timeDecay_6 = 1.0 / (log(((tomorrow - oneMonthAgo).toDouble / (1000 * 60 * 60 * 24))) + 1)
    println("一月前的衰减因子为：" + timeDecay_6)

    val timeDecay_7 = 1.0 / (log(((tomorrow - sixMonthAgo).toDouble / (1000 * 60 * 60 * 24))) + 1)
    println("半年前的衰减因子为：" + timeDecay_7)

    val timeDecay_8 = 1.0 / (log(((tomorrow - oneYearAgo).toDouble / (1000 * 60 * 60 * 24))) + 1)
    println("一年前的衰减因子为：" + timeDecay_8)



  }

}
