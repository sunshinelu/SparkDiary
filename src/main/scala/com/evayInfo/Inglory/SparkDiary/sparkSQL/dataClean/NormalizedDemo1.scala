package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

/*
均一化参考链接：https://stackoverflow.com/questions/33924842/minmax-normalization-in-scala
appRecomV1
 */
object NormalizedDemo1 {
    def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {
    SetLogger

    val SparkConf = new SparkConf().setAppName(s"getHotLabe").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df = sc.parallelize(Seq(
      (1L, 0.5), (2L, 10.2), (3L, 5.7), (4L, -11.0), (5L, 22.3)
    )).toDF("k", "v")

    val (vMin, vMax) = df.agg(min($"v"), max($"v")).first match {
      case Row(x: Double, y: Double) => (x, y)
    }

    val scaledRange = lit(2) // Range of the scaled variable
    val scaledMin = lit(-1)  // Min value of the scaled variable
    val vNormalized = ($"v" - vMin) / (vMax - vMin) // v normalized to (0, 1) range

    val vScaled = scaledRange * vNormalized + scaledMin

    df.withColumn("vScaled", vScaled).show
    /*
    +---+-----+--------------------+
|  k|    v|             vScaled|
+---+-----+--------------------+
|  1|  0.5| -0.3093093093093092|
|  2| 10.2| 0.27327327327327344|
|  3|  5.7|0.003003003003003...|
|  4|-11.0|                -1.0|
|  5| 22.3|                 1.0|
+---+-----+--------------------+
     */


    val vectorizeCol = udf( (v:Double) => Vectors.dense(Array(v)) )
    val df2 = df.withColumn("vVec", vectorizeCol(df("v")))
    val scaler = new MinMaxScaler()
      .setInputCol("vVec")
      .setOutputCol("vScaled")
      .setMax(1)
      .setMin(-1)

    scaler.fit(df2).transform(df2).show
    /*
+---+-----+-------+--------------------+
|  k|    v|   vVec|             vScaled|
+---+-----+-------+--------------------+
|  1|  0.5|  [0.5]|[-0.3093093093093...|
|  2| 10.2| [10.2]|[0.27327327327327...|
|  3|  5.7|  [5.7]|[0.00300300300300...|
|  4|-11.0|[-11.0]|              [-1.0]|
|  5| 22.3| [22.3]|               [1.0]|
+---+-----+-------+--------------------+
     */

      sc.stop()
    spark.stop()

  }
}
