package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/11/2.
 * 长宽表转换之宽表转长表
 * 参考链接：
 * Transpose column to row with Spark：https://stackoverflow.com/questions/37864222/transpose-column-to-row-with-spark
+---+-----+-----+-----+-----+
|A  |col_1|col_2|col_3|col_4|
+---+-----+-----+-----+-----+
|1  |0.0  |0.6  |0.8  |0.1  |
|1  |0.6  |0.7  |0.6  |0.4  |
|2  |0.1  |0.2  |0.3  |0.4  |
|3  |0.5  |0.2  |0.6  |0.4  |
+---+-----+-----+-----+-----+
 *
 * 转
 *
+---+-----+---+
|A  |key  |val|
+---+-----+---+
|1  |col_1|0.0|
|1  |col_2|0.6|
|1  |col_3|0.8|
|1  |col_4|0.1|
|1  |col_1|0.6|
|1  |col_2|0.7|
|1  |col_3|0.6|
|1  |col_4|0.4|
|2  |col_1|0.1|
|2  |col_2|0.2|
|2  |col_3|0.3|
|2  |col_4|0.4|
|3  |col_1|0.5|
|3  |col_2|0.2|
|3  |col_3|0.6|
|3  |col_4|0.4|
+---+-----+---+
 */
object pivotDemo2 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    val sparkConf = new SparkConf().setAppName(s"pivotDemo2").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df = Seq((1, 0.0, 0.6,0.8,0.1), (1, 0.6, 0.7,0.6,0.4),(2, 0.1, 0.2,0.3,0.4), (3, 0.5, 0.2,0.6,0.4)).
      toDF("A", "col_1", "col_2","col_3","col_4")
    df.show(false)
    println(df.dtypes.toList)

    def toLong(df: DataFrame, by: Seq[String]): DataFrame = {
      val (cols, types) = df.dtypes.filter{ case (c, _) => !by.contains(c)}.unzip
      require(types.distinct.size == 1)

      val kvs = explode(array(
        cols.map(c => struct(lit(c).alias("key"), col(c).alias("val"))): _*
      ))

      val byExprs = by.map(col(_))

      df.select(byExprs :+ kvs.alias("_kvs"): _*)
        .select(byExprs ++ Seq($"_kvs.key", $"_kvs.val"): _*)
    }

    val df1 = toLong(df, Seq("A"))
    df1.show(false)


    val by = Seq("A")
    val (cols, types) = df.dtypes.filter{ case (c, _) => !by.contains(c)}.unzip
    require(types.distinct.size == 1)
    println("cols is: " + cols.mkString(";"))
    println("types is: " + types.mkString(";"))
    val df_type = df.dtypes
    df_type.foreach(_.toString())
    val t = df.dtypes.filter{ case (c, _) => !by.contains(c)}//.unzip
    println(t.mkString(";"))
    println(types.distinct.size)

    val kvs = explode(array(
      cols.map(c => struct(lit(c).alias("key"), col(c).alias("val"))): _*
    ))
    println(kvs.toString())

    val byExprs = by.map(col(_))
    println(byExprs.toList)

    val t2 = byExprs :+ kvs.alias("_kvs")//: _*
    println(t2.toString())

    df.select(byExprs :+ kvs.alias("_kvs"): _*).show(false)
    df.select(byExprs :+ kvs.alias("_kvs"): _*).printSchema()

    df.select(byExprs :+ kvs.alias("_kvs"): _*)
      .select(byExprs ++ Seq($"_kvs.key", $"_kvs.val"): _*).show(false)

    cols.map(c => struct(lit(c).alias("key"), col(c).alias("val"))).foreach(println)

    val ta = Array(cols.map(c => struct(lit(c).alias("key"), col(c).alias("val"))):_*)
    ta.foreach(println)

    sc.stop()
    spark.stop()
  }
}
