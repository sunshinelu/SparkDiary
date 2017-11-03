package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/11/3.
 * spark dataframe中新增一列递增列
 *
 * 参考链接
 * spark dataframe :how to add a index Column：https://stackoverflow.com/questions/43406887/spark-dataframe-how-to-add-a-index-column/43408058
 */
object addIncreasedCol {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"addIncreasedCol").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df = sc.parallelize(Seq(("a","b","c", 2),("d","e","f", 3),("g","h","i", 4),
                                ("a","b","d", 9),("a","i","c", 2),("g","y","r", 5))).toDF("col1","col2","col3","col4")
    df.show(false)
    /*
+----+----+----+----+
|col1|col2|col3|col4|
+----+----+----+----+
|a   |b   |c   |2   |
|d   |e   |f   |3   |
|g   |h   |i   |4   |
|a   |b   |d   |9   |
|a   |i   |c   |2   |
|g   |y   |r   |5   |
+----+----+----+----+
     */

    df.withColumn("id",monotonicallyIncreasingId).show(false)
    /*
+----+----+----+----+-----------+
|col1|col2|col3|col4|id         |
+----+----+----+----+-----------+
|a   |b   |c   |2   |0          |
|d   |e   |f   |3   |8589934592 |
|g   |h   |i   |4   |8589934593 |
|a   |b   |d   |9   |17179869184|
|a   |i   |c   |2   |25769803776|
|g   |y   |r   |5   |25769803777|
+----+----+----+----+-----------+
     */

    /*
    新增一列column index列
     */
    val w1 = Window.orderBy("col1")
    val df1 = df.withColumn("id", row_number().over(w1))
    df1.show(false)
/*
+----+----+----+----+---+
|col1|col2|col3|col4|id |
+----+----+----+----+---+
|a   |b   |c   |2   |1  |
|a   |b   |d   |9   |2  |
|a   |i   |c   |2   |3  |
|d   |e   |f   |3   |4  |
|g   |h   |i   |4   |5  |
|g   |y   |r   |5   |6  |
+----+----+----+----+---+
 */

    /*
    新增一列分组后排序列
     */
    val w2 = Window.partitionBy("col1").orderBy("col1")
    val df2 = df.withColumn("id", row_number().over(w2))
    df2.show(false)
    /*
+----+----+----+----+---+
|col1|col2|col3|col4|id |
+----+----+----+----+---+
|g   |h   |i   |4   |1  |
|g   |y   |r   |5   |2  |
|d   |e   |f   |3   |1  |
|a   |b   |c   |2   |1  |
|a   |b   |d   |9   |2  |
|a   |i   |c   |2   |3  |
+----+----+----+----+---+
     */

    val w3 = Window.orderBy($"col4".cast("long")).rangeBetween(0, 9)//此方法报错
    val df3 = df.withColumn("id", row_number().over(w3))
    df3.show(false)



    sc.stop()
    spark.stop()
  }
}
