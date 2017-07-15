package com.evayInfo.Inglory.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
 * Created by sunlu on 17/6/9.
 */
object timeSeries extends Serializable {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(s"timeSeries").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)
    //    println(today)


    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -0)
    var threeDays = dateFormat.format(cal.getTime())
    //    println(threeDays)

    //一个空的数组缓冲，准备存放整数
    val ab = ArrayBuffer[String]()

    val n = 12
    for (i <- 0 to n) {
      //定义时间格式
      // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
      var cal: Calendar = Calendar.getInstance()
      cal.add(Calendar.DATE, -i)
      var iDaysAgo = dateFormat.format(cal.getTime())
      ab += iDaysAgo
    }
    //    println(ab)
    //    ab.foreach(println)

    val arr1 = sc.parallelize(ab).toDF("timeSeries")
    arr1.show()
    val labelUdf = udf((arg: String) => {
      "正类,负类,中性"
    })


    val ds1 = arr1.withColumn("label", labelUdf($"timeSeries")).as[(String, String)]
    val ds2ColumnsName = Seq("time", "labels", "label")
    val ds2 = ds1.flatMap {
      case (timeSeries, labels) => labels.split(",").map((timeSeries, labels, _))
    }.toDF(ds2ColumnsName: _*)
    ds2.printSchema()
    ds2.show()

    println("========")
    gTimeSeries(15).foreach(println)


  }

  def gTimeSeries(n: Int): ArrayBuffer[String] = {
    //一个空的数组缓冲，准备存放时间串
    val ab = ArrayBuffer[String]()
    for (i <- 0 to n) {
      //定义时间格式
      // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
      var cal: Calendar = Calendar.getInstance()
      cal.add(Calendar.DATE, -i)
      //获取N天前的日期
      var iDaysAgo = dateFormat.format(cal.getTime())
      // 将时间串放入变长数组ab中
      ab += iDaysAgo
    }
    ab
  }

  def getTS(n: Int): ArrayBuffer[String] = {
    //一个空的数组缓冲，准备存放时间串
    val ab = ArrayBuffer[String]()
    for (i <- 0 to n) {
      //定义时间格式
      // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
      var cal: Calendar = Calendar.getInstance()
      cal.add(Calendar.DATE, -i)
      //获取N天前的日期
      var iDaysAgo = dateFormat.format(cal.getTime())
      // 将时间串放入变长数组ab中
      ab += iDaysAgo
    }
    ab
  }

}
