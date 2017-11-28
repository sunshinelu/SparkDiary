package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/11/28.
 * 在spark dataframe处理时间差
 * string类型转时间类型
 * 时间格式转换
 * 时间差计算
 */
object TimeIntervalDemo2 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  case class Col1(id: String, name:String,time:String)

  def main(args: Array[String]) {
    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"TimeIntervalDemo2").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df1 = spark.createDataset(Seq(Col1("1", "a","2017-11-20"),Col1("2", "b","2017-11-20"),
      Col1("3", "c","2017-11-20"),Col1("4", "d","2017-11-20"),Col1("5", "e","2017-11-20"),
      Col1("6", "f","2017-11-20")))
    df1.show(false)

    // 获取当前时间
    val df2 = df1.withColumn("currTime", current_timestamp()).withColumn("currTime", date_format($"currTime", "yyyy-MM-dd HH:mm:ss"))
    df2.show(false)
    df2.printSchema()

    // 将string类型的时间串转成timestamp类型
    val df3 = df2.withColumn("timestamp", col("currTime").cast("timestamp"))
    df3.show(false)
    df3.printSchema()

    // 将string类型的时间串转成date类型
    val df4 = df2.withColumn("date", col("currTime").cast("date"))
    df4.show(false)
    df4.printSchema()


    // 计算时间差，以天为单位
    val df5 = df4.withColumn("datediff", datediff(col("date"), col("time")))
    df5.show()
    df5.printSchema()

    val df6 = df4.withColumn("datediff", datediff(col("currTime"), col("time")))
    df6.show()
    df6.printSchema()

    // 新增加一天
    val df7 = df4.withColumn("dateadd", date_add(col("currTime"), 1))
    df7.show(false)
    df7.printSchema()

    sc.stop()
    spark.stop()
  }
}
