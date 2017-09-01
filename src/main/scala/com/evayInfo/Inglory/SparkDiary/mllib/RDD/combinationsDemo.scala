package com.evayInfo.Inglory.SparkDiary.mllib.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/9/1.
 */
object combinationsDemo {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"combinationsDemo").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val list = List(1, 2, 3)
    val combs = list.combinations(2)
    combs.foreach(println)
    /*
List(1, 2)
List(1, 3)
List(2, 3)
     */

    val combs2 = list.reverse.combinations(2)
    combs2.foreach(println)
    /*
List(3, 2)
List(3, 1)
List(2, 1)
     */

    val list2 = List("a", "b", "d", "f", "c")
    //对list2进行排序后再进行combinations
    val combs3 = list2.sorted.combinations(3)
    combs3.foreach(println)
    /*
List(a, b, c)
List(a, b, d)
List(a, b, f)
List(a, c, d)
List(a, c, f)
List(a, d, f)
List(b, c, d)
List(b, c, f)
List(b, d, f)
List(c, d, f)
     */
    println("=========")
    // 未进行排序，直接进行combinations：
    val combs4 = list2.combinations(3)
    combs4.foreach(println)
    /*
List(a, b, d)
List(a, b, f)
List(a, b, c)
List(a, d, f)
List(a, d, c)
List(a, f, c)
List(b, d, f)
List(b, d, c)
List(b, f, c)
List(d, f, c)   */

    sc.stop()
    spark.stop()
  }

}
