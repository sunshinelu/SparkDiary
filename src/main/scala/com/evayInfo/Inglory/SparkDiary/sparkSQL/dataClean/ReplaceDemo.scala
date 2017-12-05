package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ReplaceDemo {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {
    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"ReplaceDemo").setMaster("local[*]").set("spark.executor.memory", "2g")
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
    val df2 = df1.groupBy("col1").max("col2").withColumnRenamed("max(col2)","col3")
    df2.show(false)
/*
+----+----+
|col1|col3|
+----+----+
|b   |2   |
|a   |3   |
+----+----+

 */
    val df3 = df1.join(df2,Seq("col1"), "left")
    df3.show(false)
/*
+----+----+----+
|col1|col2|col3|
+----+----+----+
|a   |1   |3   |
|a   |2   |3   |
|a   |3   |3   |
|b   |1   |2   |
|b   |2   |2   |
+----+----+----+
 */
    val df4 = df3.withColumn("col4", $"col2" / $"col3").
      withColumn("col4", $"col4".cast("int")).
      withColumn("col4", $"col4".cast("string"))
    df4.show(false)
/*
+----+----+----+----+
|col1|col2|col3|col4|
+----+----+----+----+
|a   |1   |3   |0   |
|a   |2   |3   |0   |
|a   |3   |3   |1   |
|b   |1   |2   |0   |
|b   |2   |2   |1   |
+----+----+----+----+
 */

    val df5 = df4.withColumn("col4", regexp_replace($"col4","1","true"))
        .withColumn("col4", regexp_replace($"col4","0","false"))
    df5.show(false)
/*
+----+----+----+-----+
|col1|col2|col3|col4 |
+----+----+----+-----+
|a   |1   |3   |false|
|a   |2   |3   |false|
|a   |3   |3   |true |
|b   |1   |2   |false|
|b   |2   |2   |true |
+----+----+----+-----+
 */
    df4.withColumn("col5", regexp_replace($"col4","1","true"))
      .withColumn("col5", regexp_replace($"col4","0","false")).show(false)
/*
+----+----+----+----+-----+
|col1|col2|col3|col4|col5 |
+----+----+----+----+-----+
|a   |1   |3   |0   |false|
|a   |2   |3   |0   |false|
|a   |3   |3   |1   |1    |
|b   |1   |2   |0   |false|
|b   |2   |2   |1   |1    |
+----+----+----+----+-----+
 */
    df4.withColumn("col5", regexp_replace($"col4","0","false"))
      .withColumn("col5", regexp_replace($"col4","1","true")).show(false)
/*
+----+----+----+----+----+
|col1|col2|col3|col4|col5|
+----+----+----+----+----+
|a   |1   |3   |0   |0   |
|a   |2   |3   |0   |0   |
|a   |3   |3   |1   |true|
|b   |1   |2   |0   |0   |
|b   |2   |2   |1   |true|
+----+----+----+----+----+
 */

    sc.stop()
    spark.stop()
  }
}
