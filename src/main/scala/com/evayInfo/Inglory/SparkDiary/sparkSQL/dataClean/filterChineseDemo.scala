package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import com.evayInfo.Inglory.SparkDiary.database.hbase.checkWebsitelb.SetLogger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object filterChineseDemo {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {

    SetLogger
    val sparkConf = new SparkConf().setAppName(s"swt: checkWebsitelb").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df = sc.parallelize(Seq(
      (0, "中文", 30.9), (0, "cat13", 22.1), (0, "cat95", 19.6), (0, "cat105", 1.3),
      (1, "cat67", 28.5), (1, "中文", 26.8), (1, "中文", 12.6), (1, "cat23", 5.3),
      (2, "cat56", 39.6), (2, "cat40", 29.7), (2, "中文", 27.9), (2, "中文", 9.8),
      (3, "cat8", 35.6))).toDF("Hour", "Category", "TotalValue")

    def filterChinese(input: String): String = {
      //      val output = input.replaceAll("[\\u4e00-\\u9fa5]*", "1")
      //      val output = input.replaceAll("[^(\\u4e00-\\u9fa5)]*", "1")
      val regx ="""[\u4e00-\u9fa5]+""".r
      val output = regx.replaceAllIn(input, "汉字")

      return output
    }

    val filterChineseUDF = udf((col: String) => filterChinese(col))

    val df1 = df.withColumn("whether", filterChineseUDF($"Category"))
    df1.show()
    /*
    +----+--------+----------+-------+
|Hour|Category|TotalValue|whether|
+----+--------+----------+-------+
|   0|      中文|      30.9|     汉字|
|   0|   cat13|      22.1|  cat13|
|   0|   cat95|      19.6|  cat95|
|   0|  cat105|       1.3| cat105|
|   1|   cat67|      28.5|  cat67|
|   1|      中文|      26.8|     汉字|
|   1|      中文|      12.6|     汉字|
|   1|   cat23|       5.3|  cat23|
|   2|   cat56|      39.6|  cat56|
|   2|   cat40|      29.7|  cat40|
|   2|      中文|      27.9|     汉字|
|   2|      中文|       9.8|     汉字|
|   3|    cat8|      35.6|   cat8|
+----+--------+----------+-------+

     */

    def filterNumber(input:String):String = {
      val regx = """[0-9]+""".r
      val output = regx.replaceAllIn(input, "数字")
      return output
    }
    val filterNumberUDF = udf((col: String) => filterNumber(col))
    val df2 = df.withColumn("whether", filterNumberUDF($"Category"))
    df2.show()
    /*

     */

    sc.stop()
    spark.stop()
  }
}
