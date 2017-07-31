package com.evayInfo.Inglory.TestCode


import com.evayInfo.Inglory.util.DateUtils

/**
 * Created by sunlu on 17/7/31.
 */
object DateTest {

  def main(args: Array[String]) {
    val s = "3小时前"
    val tt1 = DateUtils.parseTime_weibo(s)
    println(tt1)


  }

}
