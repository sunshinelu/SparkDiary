package com.evayInfo.Inglory.SparkDiary.files

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Created by sunlu on 18/10/17.
 */
object readFolder {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val SparkConf = new SparkConf().setAppName(s"readFolder").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val path = "/Users/sunlu/Documents/Case/天枢大数据平台V3.0/数据挖掘分析平台/数据挖掘平台所需demo数据/bigDataMiner_demodata/05文本分类测试数据/搜狗文本分类语料库迷你版/*/*"
    val rdd = sc.wholeTextFiles(path)

    /*
    replace(" ", ""); 去掉所有空格，包括首尾、中间
    replaceAll("\\s*", "") 可以替换大部分空白字符， 不限于空格
    \s 可以匹配空格、制表符、换页符等空白字符的其中任意一个
     */
    val df = rdd.map{x => (x._1.split("/")(11),x._2.replaceAll("\\s", "").replace("&nbsp;", "").replace("　　",""))}.
      toDF("label","txt")
//    df.show(truncate = false)

    val url = "jdbc:mysql://localhost:3306/data_mining_DB?useUnicode=true&characterEncoding=UTF-8&" +
      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val user = "root"
    val password = "root"
    val prop = new Properties()
    prop.setProperty("user", user)
    prop.setProperty("password", password)
    val opt_table = "Sogou_Classification_mini"

    df.write.mode(SaveMode.Append).jdbc(url, opt_table, prop)

    sc.stop()
    spark.stop()
  }

}
