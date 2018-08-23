package com.evayInfo.Inglory.SparkDiary.TimeSeries

import java.sql.Timestamp

import com.cloudera.sparkts.models.ARIMA
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, TimestampType}
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}

import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests
import com.cloudera.sparkts._


/**
 * Created by sunlu on 18/8/22.
 */
object TimeSeriesDemo3 {

  // do not print log
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  def main(args: Array[String]) {
    SetLogger
    val conf = new SparkConf().setAppName(s"TimeSeriesDemo3").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    val df1 = Seq(
      ("2018-01-01 00:00:00","k1",0.5),
      ("2018-01-02 00:00:00","k2",0.7),
      ("2018-01-03 00:00:00","k3",0.9),
      ("2018-01-02 00:00:00","k3",0.5),
      ("2018-01-03 00:00:00","k1",0.7),
      ("2018-01-01 00:00:00","k2",0.9)).toDF("time","id","value")
    df1.show()

    val df2 = df1.withColumn("time", $"time".cast(TimestampType)).withColumn("value", $"value".cast(DoubleType))
    df2.show(truncate = false)

    val zoneId = ZoneId.systemDefault()
    val dtIndex:UniformDateTimeIndex=DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(2018, 1, 1, 0, 0, 0, 0,zoneId),
      ZonedDateTime.of(2018, 1, 3, 0, 0, 0, 0, zoneId),
      new DayFrequency(1))

    println(dtIndex.first)

    val tsRDD = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, df2, "time", "id", "value").fill("zero")

    /*Compute Durbin-Watson stats for each series*/
    val dwStats = tsRDD.mapValues(TimeSeriesStatisticalTests.dwtest)

    dwStats.foreach(println)

    println(dwStats.map(_.swap).min)
    println(dwStats.map(_.swap).max)

//    val modelsRdd = tsRDD.map { ts => ARIMA.autoFit(ts._2)
//    }




    sc.stop()
    spark.stop()

  }

}
