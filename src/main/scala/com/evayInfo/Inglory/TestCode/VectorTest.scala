package com.evayInfo.Inglory.TestCode

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/8/29.
 */
object VectorTest {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"VectorTest").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val a = Vectors.dense(1, 2, 3)
    val b = Vectors.dense(4, 5, 6)

    // 这两个向量怎么连接得到Vectors.dense(1,2,3,4,5,6)

  }
}
