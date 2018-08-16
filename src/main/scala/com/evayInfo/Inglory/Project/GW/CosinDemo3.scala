package com.evayInfo.Inglory.Project.GW

import breeze.linalg.DenseVector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.FloatType
import scala.collection.mutable.ListBuffer
import scala.collection.mutable
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 18/8/14.
 * 使用spark中的dataframe计算余弦相似度
 */
object CosinDemo3 {
  // do not print log
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
  /**
   * 求向量的模
   * @param vec
   * @return
   */
  def module(vec: Vector[Double]) = {
    math.sqrt(vec.map(math.pow(_, 2)).sum)
  }

  /**
   * 求两个向量的内积
   * @param v1
   * @param v2
   * @return
   */
  def innerProduct(v1: Vector[Double], v2: Vector[Double]) = {
    val listBuffer = ListBuffer[Double]()
    for (i <- 0 until v1.length; j <- 0 to v2.length; if i == j) {
      if (i == j) listBuffer.append(v1(i) * v2(j))
    }
    listBuffer.sum
  }
  /**
   * 求两个向量的余弦
   * @param v1
   * @param v2
   * @return
   */
  def cosvec(v1: Vector[Double], v2: Vector[Double]) = {
    val cos = innerProduct(v1, v2) / (module(v1) * module(v2))
    if (cos <= 1) cos else 1.0
  }

  def main(args: Array[String]) {
    SetLogger
    val conf = new SparkConf().setAppName(s"CosinDemo3").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val df = spark.createDataFrame(Seq(
      (0, Array(1.0, 3.0, 3.0)),
      (1, Array(1.0, 2.0, 3.0))
    )).toDF("id", "vec")
    df.printSchema()

    val arr = Vector(1.0, 3.0, 3.0)

//    val cosin_udf = udf(() => cosvec(col("args"),col("args")))
//
//    val df2 = df.withColumn("coSim", udf(cosvec, FloatType)(col("myCol"), Array((lit(v) for v in arr])))

    val arr2 = Array(for(i <- arr) {
      lit(i)
    })

    println(arr2)

//    println(lit(arr))




    sc.stop()
    spark.stop()
  }

}
