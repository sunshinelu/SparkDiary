package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{MaxAbsScaler, MinMaxScaler, StandardScaler, Normalizer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/10/30.
 * 使用Spark MLlib中的函数进行标准化处理
 */
object NormalizedDemo2 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  def main(args: Array[String]) {
    SetLogger

    val SparkConf = new SparkConf().setAppName(s"NormalizedDemo2").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    /*
    val dataFrame = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.5, -1.0)),
      (1, Vectors.dense(2.0, 1.0, 1.0)),
      (2, Vectors.dense(4.0, 10.0, 2.0))
    )).toDF("id", "features")
    */

    val df = sc.parallelize(Seq(
      (1L, 0.5), (2L, 10.2), (3L, 5.7), (4L, -11.0), (5L, 22.3)
    )).toDF("id", "v")

    val vectorizeCol = udf((v: Double) => Vectors.dense(Array(v)))
    val dataFrame = df.withColumn("features", vectorizeCol(df("v"))).select("id", "features")

    println("dataFrame is: ")
    dataFrame.show(false)
    println("=======================================================")

    /*
Normalizer
     */
    // Normalize each Vector using $L^1$ norm.
    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0)

    val l1NormData = normalizer.transform(dataFrame)
    println("Normalized using L^1 norm")
    l1NormData.show(false)

    // Normalize each Vector using $L^\infty$ norm.
    val lInfNormData = normalizer.transform(dataFrame, normalizer.p -> Double.PositiveInfinity)
    println("Normalized using L^inf norm")
    lInfNormData.show(false)
    println("=======================================================")

    /*
StandardScaler
     */
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    // Compute summary statistics by fitting the StandardScaler.
    val scalerModel = scaler.fit(dataFrame)

    // Normalize each feature to have unit standard deviation.
    val scaledData = scalerModel.transform(dataFrame)
    println("Normalized using StandardScaler function")
    scaledData.show(false)
    println("=======================================================")

    /*
    MinMaxScaler
     */
    val scaler_MinMax = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    // Compute summary statistics and generate MinMaxScalerModel
    val scaler_MinMax_Model = scaler_MinMax.fit(dataFrame)

    // rescale each feature to range [min, max].
    val scaled_MinMax_Data = scaler_MinMax_Model.transform(dataFrame)
    println("Normalized using MinMaxScaler function")
    println(s"Features scaled to range: [${scaler_MinMax.getMin}, ${scaler_MinMax.getMax}]")
    scaled_MinMax_Data.select("features", "scaledFeatures").show(false)
    println("=======================================================")
    /*
    MaxAbsScaler

     */

    val scaler_MaxAbs = new MaxAbsScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    // Compute summary statistics and generate MaxAbsScalerModel
    val scaler_MaxAbs_Model = scaler_MaxAbs.fit(dataFrame)

    // rescale each feature to range [-1, 1]
    val scaled_MaxAbs_Data = scaler_MaxAbs_Model.transform(dataFrame)
    println("Normalized using MaxAbsScaler function")
    scaled_MaxAbs_Data.select("features", "scaledFeatures").show(false)
    println("=======================================================")


    sc.stop()
    spark.stop()

  }
}
