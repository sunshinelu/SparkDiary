package com.evayInfo.Inglory.SparkDiary.Vector

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import breeze.linalg._
import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.{DenseVector => SDV}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.optimization.L1Updater

/**
 * Created by sunlu on 18/8/14.
 * spark dense vector 与 breeze dense vector互转换
 * https://www.cnblogs.com/alexander-chao/p/5179871.html
 */
object DenseVectorExchange {
  // do not print log
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def SDV2BDV(vector: SDV): BDV[Double] = {
    new BDV(vector.values)
  }

  def BDV2SDV(vector: BDV[Double]): SDV = {
    new SDV(vector.data)
  }


  def main(args: Array[String]) {
    SetLogger
    val conf = new SparkConf().setAppName(s"DenseVectorExchange").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val w = new SDV(Array(1.0, 2.0, 3.0))
    val g = new SDV(Array(0.0, 1.0, 1.0))

    //此处将SDV转换为BDV可以进行进一步计算！
    axpy(2.0, SDV2BDV(w), SDV2BDV(g))


    println(g)

    sc.stop()
    spark.stop()

  }
}
