package com.evayInfo.Inglory.TestCode

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
 * Created by sunlu on 17/11/3.
 * 解决新增一列递增列问题
 */
object stringTest {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  def main(args: Array[String]) {

    SetLogger

    val sparkConf = new SparkConf().setAppName(s"stringTest").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val s1 = "abc"
    val t1 = s1 * 5
    println(t1)


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
    val ts = getTS(29)
    println(ts)
    println(ts.length)

    val ts_index = 0 to 29
    val indexed_ts = ts_index.zip(ts)
    println(indexed_ts)

    val n = 1 to 1000
    println(n.map(_ % 30).distinct.length)
    println(n.map(_ % 30).distinct)

    val df1 = sc.parallelize(indexed_ts).toDF("id", "timeSeq")
    df1.show(100, false)

    val df2 = sc.parallelize(Seq(("a","b","c"),("d","e","f"),("g","h","i"))).toDF("col1","col2","col2")
    df2.show(false)

    val w = Window.orderBy("col1")

    df2.withColumn("id", row_number().over(w)).show(false)

//    val id = sc.parallelize(Seq((1),(2),(3))).toDF("id")
    val id = spark.range(1, 10000).toDF("id")
    id.show(false)

//    val df3 = df2.union(id)
//    df3.show(false)


    sc.stop()
    spark.stop()
  }
}
