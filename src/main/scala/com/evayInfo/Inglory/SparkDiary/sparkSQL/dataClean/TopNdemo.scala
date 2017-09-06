package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/8/21.
 * 参考资料：
 * http://stackoverflow.com/questions/33878370/spark-dataframe-select-the-first-row-of-each-group
 * 《Spark MLlib机器学习》
 */
object TopNdemo {


  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"TopNdemo").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    /*
使用data frame 解决spark TopN问题：分组、排序、取TopN
注意：row_number()在spark1.x版本中为rowNumber()
 */
    val df = sc.parallelize(Seq(
      (0, "cat26", 30.9), (0, "cat13", 22.1), (0, "cat95", 19.6), (0, "cat105", 1.3),
      (1, "cat67", 28.5), (1, "cat4", 26.8), (1, "cat13", 12.6), (1, "cat23", 5.3),
      (2, "cat56", 39.6), (2, "cat40", 29.7), (2, "cat187", 27.9), (2, "cat68", 9.8),
      (3, "cat8", 35.6))).toDF("Hour", "Category", "TotalValue")

    df.show

    val w = Window.partitionBy($"hour").orderBy($"TotalValue".desc)
    val w2 = Window.partitionBy($"hour", $"Category").orderBy($"TotalValue".desc)
    //取Top1
    val dfTop1 = df.withColumn("rn", row_number.over(w)).where($"rn" === 1) //.drop("rn")
    dfTop1.show

    //取Top3
    val dfTop3 = df.withColumn("rn", row_number.over(w)).where($"rn" <= 3) //.drop("rn")
    dfTop3.show

    //取Top3
    val dfTop3_TEMP = df.withColumn("rn", row_number.over(w2)).where($"rn" <= 3) //.drop("rn")
    dfTop3_TEMP.show
    dfTop3_TEMP.printSchema()
    /*
    root
 |-- Hour: integer (nullable = true)
 |-- Category: string (nullable = true)
 |-- TotalValue: double (nullable = true)
 |-- rn: integer (nullable = true)
     */

    /*
    使用RDD解决spark TopN问题：分组、排序、取TopN
     */

    val rdd1 = sc.parallelize(Seq(
      (0, "cat26", 30.9), (0, "cat13", 22.1), (0, "cat95", 19.6), (0, "cat105", 1.3),
      (1, "cat67", 28.5), (1, "cat4", 26.8), (1, "cat13", 12.6), (1, "cat23", 5.3),
      (2, "cat56", 39.6), (2, "cat40", 29.7), (2, "cat187", 27.9), (2, "cat68", 9.8),
      (3, "cat8", 35.6)))

    val rdd2 = rdd1.map(x => (x._1, (x._2, x._3))).groupByKey()

    val N_value = 3

    val rdd3 = rdd2.map(x => {
      val i2 = x._2.toBuffer
      val i2_2 = i2.sortBy(_._2)
      if (i2_2.length > N_value) i2_2.remove(0, (i2_2.length - N_value))
      (x._1, i2_2.toIterable)
    })

    val rdd4 = rdd3.flatMap(x => {
      val y = x._2
      for (w <- y) yield (x._1, w._1, w._2)
    })

    rdd4.toDF("Hour", "Category", "TotalValue").show

    sc.stop()
    spark.stop()
  }
}
