package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/12/13.
 * https://stackoverflow.com/questions/45406762/how-to-get-the-last-row-from-dataframe
 */
object lastRowDemo1 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  def main(args: Array[String]) {

    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"lastRowDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val arr = Array((1,1),(4,2),(3,3),(2,4),(5,5),(7,6),(3,7),(5,8),(4,9),(18,10))
    var df = sc.parallelize(arr).toDF("value","timestamp")

    val df1 = df.reduce { (x, y) =>
      if (x.getAs[Int]("timestamp") > y.getAs[Int]("timestamp")) x else y
    }



    sc.stop()
    spark.stop()
  }
}
