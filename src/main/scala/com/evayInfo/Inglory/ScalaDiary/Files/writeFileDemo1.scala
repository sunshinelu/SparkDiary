package com.evayInfo.Inglory.ScalaDiary.Files

import java.io.{PrintWriter, File, FileWriter}

import scala.io.Source

/**
 * Created by sunlu on 17/12/20.
 */
object writeFileDemo1 {

  def main(args: Array[String]) {
    val s = Source.fromFile("/Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/sentimentDic/weibo/3_simplifyweibo.txt", "gbk").getLines().toList
//    s.foreach(println)
//    val n = s.length
//    println(n)

//    val out = new FileWriter("/Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/sentimentDic/weibo2/readme.txt",false)
//    for (i <- s)
//      out.write(i.toString)
//    out.close()


    val writer = new PrintWriter(new File("/Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/sentimentDic/weibo2/3_simplifyweibo.txt"))
    for(i <- s)
      writer.println(i)
    writer.close()



  }

}
