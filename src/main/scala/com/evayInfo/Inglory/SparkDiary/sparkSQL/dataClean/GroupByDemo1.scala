package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean



import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @Author: sunlu
  * @Date: 2019-05-07 08:44
  * @Version 1.0
  * groupby操作demo
  * 1. Spark Dataframe groupBy with sequence as keys arguments（输入一个sequence进行分组groupby操作）
  * 参考链接：
  * https://stackoverflow.com/questions/37524510/spark-dataframe-groupby-with-sequence-as-keys-arguments
  * https://stackoverflow.com/questions/33882894/spark-sql-apply-aggregate-functions-to-a-list-of-column
  *
  * 2. groupby agg之后拼接字符串
  * 参考链接：
  * https://community.hortonworks.com/questions/44886/dataframe-groupby-and-concat-non-empty-strings.html
  * https://stackoverflow.com/questions/31450846/concatenate-columns-in-apache-spark-dataframe
  *
  */
object GroupByDemo1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  case class Col1(id: String, name:String,tag:String)


  def main(args: Array[String]): Unit = {
    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"addCurrentTimeDemo").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df1 = spark.createDataset(Seq(
      Col1("1", "a","^_^"), Col1("2", "b","^_^"),Col1("3", "c","^_^"),
      Col1("1", "a","＊"),Col1("2", "b","＊"),Col1("3", "c","＊")))
    df1.show(false)
/*
+---+----+---+
|id |name|tag|
+---+----+---+
|1  |a   |^_^|
|2  |b   |^_^|
|3  |c   |^_^|
|1  |a   |＊  |
|2  |b   |＊  |
|3  |c   |＊  |
+---+----+---+
 */


    val df2 = df1.groupBy("id","name").agg(concat_ws("&", collect_list($"tag")))
    df2.show(truncate = false)
/*
+---+----+-------------------------------+
|id |name|concat_ws(&, collect_list(tag))|
+---+----+-------------------------------+
|2  |b   |^_^&＊                          |
|1  |a   |^_^&＊                          |
|3  |c   |^_^&＊                          |
+---+----+-------------------------------+
 */

    val l1 ="id,name".split(",").toList.toSeq.map(col(_))
    val df3 = df1.groupBy(l1:_*).agg(concat_ws("&", collect_list($"tag")))
    df3.show(truncate = false)
/*
+---+----+-------------------------------+
|id |name|concat_ws(&, collect_list(tag))|
+---+----+-------------------------------+
|2  |b   |^_^&＊                          |
|1  |a   |^_^&＊                          |
|3  |c   |^_^&＊                          |
+---+----+-------------------------------+
 */

    val l2 = Seq("id","name").map(col(_))
    val df4 = df1.groupBy(l2:_*).agg(concat_ws("&", collect_list($"tag")))
    df4.show(truncate = false)
/*
+---+----+-------------------------------+
|id |name|concat_ws(&, collect_list(tag))|
+---+----+-------------------------------+
|2  |b   |^_^&＊                          |
|1  |a   |^_^&＊                          |
|3  |c   |^_^&＊                          |
+---+----+-------------------------------+
 */


    sc.stop()
    spark.stop()
  }
}
