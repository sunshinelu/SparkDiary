package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 18/8/14.
 * 在spark中新增一列常量列
 */
object AddConstantCol {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger
    val conf = new SparkConf().setAppName(s"CosinDemo3").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val df = spark.createDataFrame(Seq(
      (0, Array(1.0, 2.0, 3.0)),
      (1, Array(1.0, 2.0, 3.0))
    )).toDF("id", "vec")
    val df1 = df.withColumn("tag", lit(1)).withColumn("tag2",lit("pos"))

    df1.show()
    df1.printSchema()

  }
}
