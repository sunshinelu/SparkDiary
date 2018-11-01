package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/10/19.
 * spark dataframe中关于NA的相关操作
 *
 * 参考链接：
 * Spark2 Dataset DataFrame空值null,NaN判断和处理
 * http://www.cnblogs.com/wwxbi/p/6011422.html
 */
object NaDemo1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    val SparkConf = new SparkConf().setAppName(s"NaDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df = sc.parallelize(Seq(
      (1L, 0.5), (2L, 10.2), (3L, 5.7), (4L, -11.0), (5L, 22.3), (null, -11.0), (5L, null)
    )).toDF("k", "v")
    df.show(false)

    /*
    删除含有null的行
     */
    val df1 = df.na.drop()
    df1.show(false)

    /*
    删除某一列含有null的行
     */
    val df2 = df.na.drop(Array("k"))
    df2.show(false)

    /*
    填充所有空值的列
     */

    val df3 = df.na.fill("NULL")
    df3.show(false)

    /*
    对指定的列空值填充
     */
    val df4 = df3.na.fill(value = "NULL", cols = Array("k","v"))
    df4.show(false)

    val df5 = df.na.fill(Map("k" -> 6L ,"v" -> 45.3) )
    df5.show()

    sc.stop()
    spark.stop()
  }

}
