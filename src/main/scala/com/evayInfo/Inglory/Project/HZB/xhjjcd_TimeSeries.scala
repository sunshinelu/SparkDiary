package com.evayInfo.Inglory.Project.HZB

import java.time.{ZoneId, ZonedDateTime}
import java.util.Properties

import com.cloudera.sparkts.models.{ARIMA, ARIMAModel}
import com.cloudera.sparkts.{TimeSeriesRDD, DateTimeIndex, MonthFrequency, UniformDateTimeIndex}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, TimestampType, StringType}

/**
 * Created by pc on 2018-08-23.
 * 需求：
 * 表中batch_id字段是时间  fwproduct_sum字段是数值，需求是：按照batch_id排序，求下一个的fwproduct_sum的值
 */
object xhjjcd_TimeSeries {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"xhjjcd_TimeSeries").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //connect mysql database
    val url1 = "jdbc:mysql://localhost:3306/jxw"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")
    //get data
    val ds1 = spark.read.jdbc(url1, "j_xhjjcb_time_lyfw_analyze", prop1).select("batch_id","fwproduct_sum").na.drop()
    val ds2 = spark.read.jdbc(url1, "j_xhjjcb_year_reason", prop1).select("batch_id","year","reason").na.drop()

    val ds3 = ds1.join(ds2,Seq("batch_id"),"left").orderBy($"batch_id".asc)
    ds3.show(truncate = false)

    val addString = udf((col1:String,col2:String) => (col1 + "-0" + col2 + "-01"))

    val ds4 = ds3.withColumn("year",$"year".cast(StringType)).
      withColumn("fwproduct_sum",$"fwproduct_sum".cast(DoubleType)).
      withColumn("reason",$"reason".cast(StringType)).
      withColumn("reason",regexp_replace($"reason","4","9")).
      withColumn("reason",regexp_replace($"reason","3","7")).
      withColumn("reason",regexp_replace($"reason","2","4")).withColumn("season",addString($"year",$"reason"))
    ds4.show()

    val ds5 = ds4.withColumn("season", $"season".cast(TimestampType)).withColumn("id_t", lit("1"))
    ds5.show(truncate = false)
    ds5.printSchema()

    val zoneId = ZoneId.systemDefault()
    val timeIndex:UniformDateTimeIndex=DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(2016, 1, 1, 0, 0, 0, 0,zoneId),
      ZonedDateTime.of(2018, 4, 1, 0, 0, 0, 0, zoneId),
      new MonthFrequency(3))

    val tsRDD = TimeSeriesRDD.timeSeriesRDDFromObservations(timeIndex, ds5, "season", "id_t", "fwproduct_sum").fill("zero")

    def arimaModelTrain[K](trainTsrdd: TimeSeriesRDD[K], predictedN: Int): RDD[Vector] = {
      /** *参数设置 ******/

      /** *创建arima模型 ***/
      //创建和训练arima模型.其RDD格式为(ArimaModel,Vector)
      val arimaAndVectorRdd: RDD[(ARIMAModel, Vector)] = trainTsrdd.map { case (key, denseVector) =>
        //      (ARIMA.autoFit(denseVector), denseVector)
        val naVector=Vectors.dense(Array[Double]())

        (ARIMA.autoFit(denseVector,1,0,1), naVector)

      }

      /** *预测出后N个的值 *****/
      val forecast = arimaAndVectorRdd.map { case (arimaModel, denseVector) => arimaModel.forecast(denseVector, predictedN)    }
      forecast
    }

    //    println(ts_rdd.collectAsTimeSeries().index.toZonedDateTimeArray().mkString(","))
    println("--------arimaModelTrain-----------")
    val foreach=arimaModelTrain(tsRDD,3)
    foreach.foreach(println)

    println(foreach.count())




    sc.stop()
    spark.stop()
  }

}