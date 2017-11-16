package com.evayInfo.Inglory.SparkDiary.sparkStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by sunlu on 17/11/16.
 * $ nc -lk 9999
 */
object StreamingTest {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  def main(args: Array[String]) {
    SetLogger
    val conf = new SparkConf().setAppName("StreamingTest")
    val ssc = new StreamingContext(conf, Seconds(10))

    val socket = args(0)
    val filePath = args(1)

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream(socket, 9999)//192.168.37.16  localhost
    // Split each line into words
    val words = lines.flatMap(_.split(" "))
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)//.map(x => (x._1, x._2,getNowDate())

    // Print the first ten elements of each RDD generated in this DStream to the console
    //    println(getNowDate())
    wordCounts.print()
    wordCounts.saveAsTextFiles(filePath)

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }

}
