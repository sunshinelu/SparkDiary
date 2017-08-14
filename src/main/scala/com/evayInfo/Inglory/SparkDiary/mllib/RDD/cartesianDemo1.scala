package com.evayInfo.Inglory.SparkDiary.mllib.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/8/14.
 */
object cartesianDemo1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger
    val conf = new SparkConf().setAppName(s"cartesianDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    val a = sc.parallelize(List(1, 2, 3))

    val b = sc.parallelize(List(4, 5, 6))

    val c = a.cartesian(b)

    c.collect.foreach(println)
    /*
    (1,4)
    (1,5)
    (1,6)
    (2,4)
    (2,5)
    (2,6)
    (3,4)
    (3,5)
    (3,6)
     */

    sc.stop()
    spark.stop()
  }

}
