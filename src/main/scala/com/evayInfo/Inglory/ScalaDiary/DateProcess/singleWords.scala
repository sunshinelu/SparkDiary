package com.evayInfo.Inglory.ScalaDiary.DateProcess

/**
 * Created by sunlu on 18/1/24.
 *
 * 将中文字符串分成一个字一个字，例如
 * 今天天气很好！ => 今 天 天 气 很 好 ！
 *
 *
 */
object singleWords {
  def main(args: Array[String]) {

    val s = "今天天气很好！"
    println(s.toCharArray.toList)// List(今, 天, 天, 气, 很, 好, ！)
  }
}
