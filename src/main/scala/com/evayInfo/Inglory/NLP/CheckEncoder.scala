package com.evayInfo.Inglory.NLP

/**
 * Created by sunlu on 17/10/27.
 * 参考链接
 * java可供判断某字符串是什么编码的一行代码：http://www.cnblogs.com/_popc/p/3384030.html
 */
object CheckEncoder {
  def main(args: Array[String]) {

    println("中文".getBytes())

    println(new String("中文".getBytes()))

    println(new String("中文".getBytes(), "GB2312"))

    println(new String("中文".getBytes(), "ISO8859_1"))

    println(new String("中文".getBytes("GB2312")))

    println(new String("中文".getBytes("utf-8")))

    println(new String("中文".getBytes(), "utf-8"))



  }
}
