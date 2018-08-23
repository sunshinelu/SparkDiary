package com.evayInfo.Inglory.SparkDiary.TimeSeries

import com.cloudera.sparkts.models.ARIMA
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.ml.linalg.{Vector => MLVector}

/**
 * Created by sunlu on 18/8/22.
 *
 * https://github.com/sryza/spark-ts-examples/blob/master/jvm/src/main/scala/com/cloudera/tsexamples/SingleSeriesARIMA.scala
 */
object TimeSeriesDemo4 {
  def main(args: Array[String]): Unit = {
    // The dataset is sampled from an ARIMA(1, 0, 1) model generated in R.
    val lines = scala.io.Source.fromFile("/Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/ts_data/R_ARIMA_DataSet1.csv").getLines()
    val ts = Vectors.dense(lines.map(_.toDouble).toArray)
    val arimaModel = ARIMA.fitModel(1, 0, 1, ts)
    println("coefficients: " + arimaModel.coefficients.mkString(","))
    val forecast = arimaModel.forecast(ts, 20)
    println("forecast of next 20 observations: " + forecast.toArray.mkString(","))
  }
}
