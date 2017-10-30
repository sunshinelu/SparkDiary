package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/10/30.
 */
object NormalizedDemo3 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val SparkConf = new SparkConf().setAppName(s"NormalizedDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df = sc.parallelize(Seq(
      (1L, 0.5), (2L, 10.2), (3L, 5.7), (4L, -11.0), (5L, 22.3)
    )).toDF("k", "v")

    val vectorizeCol = udf((v: Double) => Vectors.dense(Array(v)))
    val df2 = df.withColumn("vVec", vectorizeCol(df("v")))
    val scaler = new MinMaxScaler()
      .setInputCol("vVec")
      .setOutputCol("vScaled")
      .setMax(1)
      .setMin(0)

    val df3 = scaler.fit(df2).transform(df2)
    df3.show(false)

    val three= Vector(3.0, 3.0, 3.0, 3.0, 3.0)

    val df4 = df3.//withColumn("3vScaled", $"vScaled" * 3.0).
      withColumn("add_Scaled", $"vScaled" + $"vScaled")
    df4.show(false)

    sc.stop()
    spark.stop()

  }

}
