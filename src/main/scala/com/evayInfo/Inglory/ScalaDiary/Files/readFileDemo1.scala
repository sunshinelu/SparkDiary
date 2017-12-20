package com.evayInfo.Inglory.ScalaDiary.Files

import scala.io.Source

/**
 * Created by sunlu on 17/12/8.
 * 参考链接：
 * Scala 文件操作
 * http://blog.csdn.net/power0405hf/article/details/50441836
 */
object readFileDemo1 {
  def main(args: Array[String]) {
    val s = Source.fromFile("/Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/sentimentDic/posdict.txt", "UTF-8").getLines().toList
    s.foreach(println)

    val n = s.length
    println(n)

    println(s(0))
    println(s(n - 1))

  }

}
