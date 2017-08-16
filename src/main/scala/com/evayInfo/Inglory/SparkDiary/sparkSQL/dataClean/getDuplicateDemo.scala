package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/8/16.
 * 获取dataframe中的重复数据
 * 参考链接：
 * https://stackoverflow.com/questions/40026335/in-spark-dataframe-how-to-get-duplicate-records-and-distinct-records-in-two-data
 */
object getDuplicateDemo {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    val conf = new SparkConf().setAppName(s"checkTrendV1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    /*
    方法一：
    参考链接：
    https://stackoverflow.com/questions/40026335/in-spark-dataframe-how-to-get-duplicate-records-and-distinct-records-in-two-data

     */

    val acctDF = List(("1", "Acc1"), ("1", "Acc1"), ("1", "Acc1"), ("2", "Acc2"), ("2", "Acc2"), ("3", "Acc3")).toDF("AcctId", "Details")
    acctDF.show()
    /*
+------+-------+
|AcctId|Details|
+------+-------+
|     1|   Acc1|
|     1|   Acc1|
|     1|   Acc1|
|     2|   Acc2|
|     2|   Acc2|
|     3|   Acc3|
+------+-------+
     */

    val countsDF = acctDF.rdd.map(rec => (rec(0), 1)).reduceByKey(_ + _).map(rec => (rec._1.toString, rec._2)).toDF("AcctId", "AcctCount")

    val accJoinedDF = acctDF.join(countsDF, acctDF("AcctId") === countsDF("AcctId"), "left_outer").select(acctDF("AcctId"), acctDF("Details"), countsDF("AcctCount"))

    accJoinedDF.show()
    /*
+------+-------+---------+
|AcctId|Details|AcctCount|
+------+-------+---------+
|     3|   Acc3|        1|
|     1|   Acc1|        3|
|     1|   Acc1|        3|
|     1|   Acc1|        3|
|     2|   Acc2|        2|
|     2|   Acc2|        2|
+------+-------+---------+
     */

    val distAcctDF = accJoinedDF.filter($"AcctCount" === 1)
    distAcctDF.show()
    /*
+------+-------+---------+
|AcctId|Details|AcctCount|
+------+-------+---------+
|     3|   Acc3|        1|
+------+-------+---------+

     */

    /*
    方法二：

     */

    val acctDF2 = List(("1", "Acc1"), ("1", "Acc1"), ("1", "Acc1"), ("2", "Acc2"), ("2", "Acc2"), ("3", "Acc3")).toDF("AcctId", "Details")
    /*
+------+-------+
|AcctId|Details|
+------+-------+
|     1|   Acc1|
|     1|   Acc1|
|     1|   Acc1|
|     2|   Acc2|
|     2|   Acc2|
|     3|   Acc3|
+------+-------+
     */
    val acctDF2Dup = acctDF2.withColumn("value", lit(1)).groupBy("AcctId", "Details").agg(sum("value")).filter($"sum(value)" > 1)
    acctDF2Dup.show()
    /*
+------+-------+----------+
|AcctId|Details|sum(value)|
+------+-------+----------+
|     1|   Acc1|         3|
|     2|   Acc2|         2|
+------+-------+----------+
     */



    sc.stop()
    spark.stop()
  }

}
