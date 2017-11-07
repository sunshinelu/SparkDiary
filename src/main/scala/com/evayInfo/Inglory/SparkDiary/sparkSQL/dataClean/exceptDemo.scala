package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by sunlu on 17/7/26.
  */
object exceptDemo {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger
    val conf = new SparkConf().setAppName(s"exceptDemo").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df1ColumnsName = Seq("id", "s1", "s2")
    val df1 = sc.parallelize(Seq((1, "a", "a"), (2, "b", "b"), (3, "c", "c"), (4, "d", "d"))).toDF(df1ColumnsName: _*)
    df1.show()
    /*
 +---+---+---+
| id| s1| s2|
+---+---+---+
|  1|  a|  a|
|  2|  b|  b|
|  3|  c|  c|
|  4|  d|  d|
+---+---+---+
     */


    val df2ColumnsName = Seq("id", "s1", "s2")
    val df2 = sc.parallelize(Seq((1, "a", "a"), (2, "b", "b"))).toDF(df2ColumnsName: _*)
    df2.show()
    /*
+---+---+---+
| id| s1| s2|
+---+---+---+
|  1|  a|  a|
|  2|  b|  b|
+---+---+---+
     */
    df1.except(df2).show()
    /*
  +---+---+---+
| id| s1| s2|
+---+---+---+
|  4|  d|  d|
|  3|  c|  c|
+---+---+---+
     */


    val df3ColumnsName = Seq("id")
    val df3 = sc.parallelize(Seq((1), (2))).toDF(df3ColumnsName: _*)

    println("df1 leftanti结果为：")
    df1.join(df3, Seq("id"), "leftanti").show()
    /*
    df1 leftanti结果为：
    +---+---+---+
    | id| s1| s2|
    +---+---+---+
    |  3|  c|  c|
    |  4|  d|  d|
    +---+---+---+
     */

    println("df1 leftsemi结果为：")
    df1.join(df3, Seq("id"), "leftsemi").show()
    /*
    df1 leftsemi结果为：
    +---+---+---+
    | id| s1| s2|
    +---+---+---+
    |  1|  a|  a|
    |  2|  b|  b|
    +---+---+---+
     */

    println("df3 leftanti结果为：")
    df3.join(df1, Seq("id"), "leftanti").show()
    /*
 df3 leftanti结果为：
+---+
| id|
+---+
+---+
     */


    println("left_outer结果为：")
    df1.join(df3, Seq("id"), "left_outer").show()
    /*
 left_outer结果为：
+---+---+---+
| id| s1| s2|
+---+---+---+
|  1|  a|  a|
|  3|  c|  c|
|  4|  d|  d|
|  2|  b|  b|
+---+---+---+
     */

    df3.show()
    /*
+---+
| id|
+---+
|  1|
|  2|
+---+
     */
    //    df1.except(df3).show()

    sc.stop()
    spark.stop()

  }
}
