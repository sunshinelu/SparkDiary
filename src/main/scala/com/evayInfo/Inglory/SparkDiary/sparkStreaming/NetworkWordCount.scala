package com.evayInfo.Inglory.SparkDiary.sparkStreaming

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{StreamingContext, Seconds}

/**
 * Created by sunlu on 17/11/9.
 * $ nc -lk 9999
 *
 */
object NetworkWordCount {
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
/*
    val sparkConf = new SparkConf().setAppName(s"NetworkWordCount").setMaster("local[*]")//.set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    /*
     Only one SparkContext may be running in this JVM (see SPARK-2243). To ignore this error, set spark.driver.allowMultipleContexts = true. The currently running SparkContext was created at:
org.apache.spark.sql.SparkSession$Builder.getOrCreate(SparkSession.scala:860)
     */
*/
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))
    // Count each word in each batch
    val pairs = words.map(word => (word, getNowDate()))
    val wordCounts = pairs.reduceByKey(_ + _)//.map(x => (x._1, x._2,getNowDate())

    // Print the first ten elements of each RDD generated in this DStream to the console
    println(getNowDate())
    wordCounts.print()



    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

//    sc.stop()
//    spark.stop()
  }

}
