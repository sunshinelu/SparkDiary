package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/12/13.
 */
object limitDemo1 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  def main(args: Array[String]) {

    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"limitDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df1 = spark.createDataFrame(Seq(("a",1),("a",2),("a",3),("b",1),("b",2))).toDF("col1","col2")
    df1.show(false)

    val df2 = df1.head(3)
    df2.foreach(println)

    val df3 = df1.limit(3)
    df3.show()




    sc.stop()
    spark.stop()

  }

}
