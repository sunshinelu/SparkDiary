package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 17/12/6.
 *
 * 在dataframe中使用map
 *
 */
object mapDemo1 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"mapDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df1 = spark.createDataset(Seq(("a",1),("a",2),("a",3),("b",1),("b",2))).toDF("col1","col2")
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
    val df2 = df1.map{case Row(col1:String, col2: Int) => (col1, col2 + 1)}.toDF("newCol1", "newCol1")
    df2.show(false)
/*
+-------+-------+
|newCol1|newCol1|
+-------+-------+
|a      |2      |
|a      |3      |
|a      |4      |
|b      |2      |
|b      |3      |
+-------+-------+
 */


    sc.stop()
    spark.stop()
  }

}
