package com.evayInfo.Inglory.SparkDiary.sparkStreaming

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/11/13.
 */
object StructuredNetworkWordCount {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  //获取今天日期
  def getNowDate(): String = {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var today = dateFormat.format(now)
    val todayL = dateFormat.parse(today).getTime
    today
  }

  def main(args: Array[String]) {
    SetLogger

    val spark = SparkSession
      .builder()
      .config(new SparkConf().setMaster("local[2]"))
      .appName(getClass.getName)
      .getOrCreate()
    import spark.implicits._
    val lines = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",9999)
      .option("includeTimestamp", true) //输出内容包括时间戳
      .load()
    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))
    // Generate running word count
    val wordCounts = words.groupBy("value").count()
    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination()


  }
}
