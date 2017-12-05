package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by sunlu on 17/12/5.
  * 解决新增一列递增列问题
  */

object addIncreaseCol {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {
    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"addIncreaseCol").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df1 = spark.createDataFrame(Seq(("a",1),("a",2),("a",3),("b",1),("b",2))).toDF("col1","col2")
    df1.show(false)
/*
+----+----+
|col1|col2|
+----+----+
|a   |1   |
|a   |2   |
|a   |3   |
|b   |1   |
|b   |2   |
+----+----+
 */
    // 新增一列递增列
    val w1 = Window.orderBy("col1")
    val df2 = df1.withColumn("col3", row_number().over(w1))
    df2.show(false)
/*
+----+----+----+
|col1|col2|col3|
+----+----+----+
|a   |1   |1   |
|a   |2   |2   |
|a   |3   |3   |
|b   |1   |4   |
|b   |2   |5   |
+----+----+----+
 */

    // 根据col1升序，col2降序排序，新增一列递增列
    val w2 = Window.orderBy(col("col1"),col("col2").desc)
    df1.withColumn("col3", row_number().over(w2)).show(false)
/*
+----+----+----+
|col1|col2|col3|
+----+----+----+
|a   |3   |1   |
|a   |2   |2   |
|a   |1   |3   |
|b   |2   |4   |
|b   |1   |5   |
+----+----+----+
 */
    // 根据col1分组，col2倒序排序
    val w3 = Window.partitionBy("col1").orderBy(col("col2").desc)
    val df3 =  df1.withColumn("col3", row_number().over(w3))
      df3.show(false)
/*
+----+----+----+
|col1|col2|col3|
+----+----+----+
|b   |2   |1   |
|b   |1   |2   |
|a   |3   |1   |
|a   |2   |2   |
|a   |1   |3   |
+----+----+----+
 */

    sc.stop()
    spark.stop()
  }
}
