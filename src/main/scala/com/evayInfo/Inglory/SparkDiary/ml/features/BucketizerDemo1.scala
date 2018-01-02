package com.evayInfo.Inglory.SparkDiary.ml.features

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 18/1/2.
 *
 *  * 参考链接：
 * spark ML 中 VectorIndexer, StringIndexer等用法
 * http://blog.csdn.net/shenxiaoming77/article/details/63715525
 *
 * 分箱（分段处理）：将连续数值转换为离散类别
比如特征是年龄，是一个连续数值，需要将其转换为离散类别(未成年人、青年人、中年人、老年人），就要用到Bucketizer了。
分类的标准是自己定义的，在Spark中为split参数,定义如下：
double[] splits = {0, 18, 35,50， Double.PositiveInfinity}
将数值年龄分为四类0-18，18-35，35-50，55+四个段。
如果左右边界拿不准，就设置为，Double.NegativeInfinity， Double.PositiveInfinity，不会有错的。

Bucketizer transforms a column of continuous features to a column of
feature buckets, where the buckets are specified by users.
 *
 */
object BucketizerDemo1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"BucketizerDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

    val data = Array(-999.9, -0.5, -0.3, 0.0, 0.2, 999.9)
    val dataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val bucketizer = new Bucketizer()
      .setInputCol("features")
      .setOutputCol("bucketedFeatures")
      .setSplits(splits)

    // Transform original data into its bucket index.
    val bucketedData = bucketizer.transform(dataFrame)

    println(s"Bucketizer output with ${bucketizer.getSplits.length-1} buckets")
    bucketedData.show(false)


    sc.stop()
    spark.stop()

  }

}
