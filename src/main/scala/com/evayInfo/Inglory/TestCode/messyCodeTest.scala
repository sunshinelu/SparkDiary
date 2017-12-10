package com.evayInfo.Inglory.TestCode

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
 * Created by sunlu on 17/12/8.
 * spark读取文本文件时乱码问题
 *
 * http://blog.csdn.net/u010234516/article/details/52853214
 */
object messyCodeTest {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"messyCodeTest").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val s = "中国人"

    val s2 = new java.lang.String(s.getBytes("UTF-8"), "UTF-8")
    println("utf-8 to utf8 is: " + s2)

    val s3 = new java.lang.String(s.getBytes("UTF-8"), "GBK")
    println("utf-8 to gbk is: " + s3)

    val s4 = new java.lang.String(s3.getBytes(), "UTF-8")
    println("utf-8 to gbk to utf8 is: " + s4)

    val t1 = sc.textFile("file:////Users/sunlu/Desktop/test.txt").
//      map(x => (new java.lang.String(x.getBytes("GBK"),"UTF-8"), x.getBytes().length))
//    map(x => new String(x.getBytes, 0, x.getBytes().length, "GB2312"))
 map(x => new String(x.getBytes(), "UTF-8"))
    t1.collect().foreach(println)

    val t2 = Source.fromFile("/Users/sunlu/Desktop/test.txt", "GB2312").getLines().toList
    t2.foreach(println)

    val t3 = sc.textFile("file:////Users/sunlu/Desktop/test2.txt")
    t3.collect().foreach(println)

    val strby = s.getBytes("GB2312")
    val Str2 = new java.lang.String(strby,"utf-8")
    println(Str2)

  }
}
