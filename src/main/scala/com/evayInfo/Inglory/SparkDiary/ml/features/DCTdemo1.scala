package com.evayInfo.Inglory.SparkDiary.ml.features

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.DCT
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 18/7/23.
 */
object DCTdemo1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger
    //bulid environment
    val spark = SparkSession.builder.appName("DCTdemo1").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val data = Seq(
      Vectors.dense(0.0, 1.0, -2.0, 3.0),
      Vectors.dense(-1.0, 2.0, 4.0, -7.0),
      Vectors.dense(14.0, -2.0, -5.0, 1.0))

    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val dct = new DCT().
      setInputCol("features").
      setOutputCol("featuresDCT").
      setInverse(false)

    val dctDf = dct.transform(df)
    dctDf.select("featuresDCT").show(false)



    sc.stop()
    spark.stop()
  }

}
