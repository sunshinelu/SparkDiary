package com.evayInfo.Inglory.TestCode

/**
 * Created by sunlu on 17/9/1.
 * 截取string中的指定字符
 */
object subStringTest {

  def main(args: Array[String]) {
    val s = "【新闻资讯】不用再学SQL语言了，Saleforce用自然语言就能在数据库中查询"
    /*
    s.indexOf("】")找s字符串中"】"首次出现的位置
     */
    val sub = s.substring(s.indexOf("】") + 1, s.length)
    println(s.lastIndexOf("】")) //5
    println(sub) //不用再学SQL语言了，Saleforce用自然语言就能在数据库中查询

    val userNameUrl = "454512@hongri@4944115455d9591b274648a06303d910de"
    val beginIndex = userNameUrl.indexOf("@") + 1
    val endIndex = userNameUrl.lastIndexOf("@")
    println(userNameUrl.substring(beginIndex, endIndex)) //hongri

  }
}
