package com.evayInfo.Inglory.ScalaDiary.Files

/**
 * Created by sunlu on 17/12/8.
 * 参考链接
 * http://blog.csdn.net/u010234516/article/details/52853214
 */
object EncodingTest {
  def main(args: Array[String]) {
    val srcString = "中国人"
    val GbkBytes = srcString.getBytes("GBK")
    println("GbkBytes.length:" + GbkBytes.length)

    val UtfBytes = srcString.getBytes("UTF-8")
    println("UtfBytes.length:" + UtfBytes.length)

    for(i <- 0 to srcString.length()){
      val s = Character.valueOf(srcString.charAt(i)).toString()
      println(s + ":" + s.getBytes().length)
    }

  }
}
