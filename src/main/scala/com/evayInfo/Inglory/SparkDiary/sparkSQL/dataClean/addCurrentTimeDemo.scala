package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/11/28.
 * 新增一列当前时间列
 */
object addCurrentTimeDemo {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  case class Col1(id: String, name:String)

  def main(args: Array[String]) {
    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"addCurrentTimeDemo").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df1 = spark.createDataset(Seq(Col1("1", "a"),Col1("2", "b"),Col1("3", "c"),Col1("4", "d"),Col1("5", "e"),Col1("6", "f")))
    df1.show(false)

    val df2 = df1.withColumn("CJSJ", current_timestamp()).withColumn("CJSJ", date_format($"CJSJ", "yyyy-MM-dd HH:mm:ss"))
    df2.show(false)
    df2.printSchema()

    println(df2.describe().toString())

    sc.stop()
    spark.stop()
  }
}
