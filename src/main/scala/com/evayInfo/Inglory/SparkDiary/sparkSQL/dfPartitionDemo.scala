package com.evayInfo.Inglory.SparkDiary.sparkSQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/8/16.
 * 在data frame中重新对partition进行分区和获取partition数量
 * 参考链接：
 * http://stackoverflow.com/questions/30995699/how-to-define-partitioning-of-dataframe
 * https://codeday.me/bug/20170718/45208.html
 *
 */
object dfPartitionDemo {


  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val conf = new SparkConf().setAppName(s"dfPartitionDemo").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df = sc.parallelize(Seq(
      ("A", 1), ("B", 2), ("A", 3), ("C", 1)
    )).toDF("k", "v")

    val partitioned = df.repartition($"k")
    partitioned.explain

    println("***************")
    val partitioned2 = df.repartition($"v")
    partitioned2.explain

    println(spark.version)

    sc.stop()
    spark.stop()
  }

}
