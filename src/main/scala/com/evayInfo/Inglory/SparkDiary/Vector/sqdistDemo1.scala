package com.evayInfo.Inglory.SparkDiary.Vector

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 18/8/16.
 */
object sqdistDemo1 {

  // do not print log
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  def main(args: Array[String]) {
    SetLogger
    val conf = new SparkConf().setAppName(s"sqdistDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    val v1 = Vectors.dense(Array(1.0,2.0,3.0,4.0,5.0))
    val v2 = Vectors.dense(Array(1.0,2.0,2.0,4.0,5.0))
    val v3 = Vectors.dense(Array(0.0,2.0,2.0,4.0,9.0))


    val sqdist_t1 = Vectors.sqdist(v1, v2)
    println("sqdist_t1 is: " + sqdist_t1)
    //sqdist_t1 is: 1.0

    val sqdist_t2 = Vectors.sqdist(v1,v1)
    println("sqdist_t2 is: " + sqdist_t2)

    val sqdist_t3 = Vectors.sqdist(v1,v3)
    println("sqdist_t3 is: " + sqdist_t3)
    // sqdist_t3 is: 18.0

    sc.stop()
    spark.stop()
  }

}
