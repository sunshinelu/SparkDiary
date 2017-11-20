package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/11/20.
 */
object MergeDemo1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  def main(args: Array[String]) {

    SetLogger

    val sparkConf = new SparkConf().setAppName(s"MergeDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df1 = spark.createDataFrame(Seq(
      (0,0,"a0",0.0),
      (1,1,"a1",1.0),
      (2,2,"a2",2.0)
    ))

    df1.show(false)

    val df2 = df1.groupByKey(_.getInt(0)).mapGroups((key,iter) => iter.length)
    df2.rdd.foreach(println)

    val df3 = df1.groupByKey(_.getInt(0)).mapGroups((key,iter) => (key,iter))
    df3.show(false)

    sc.stop()
    spark.stop()
  }

}
