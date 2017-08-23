package com.evayInfo.Inglory.TestCode


import breeze.linalg.{DenseMatrix, DenseVector, norm}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/8/23.
 */
object cosineSimilarity {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"cosineSimilarity").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext


    //    val v1 = Vectors.dense(0.1, 0.2, 0.3)
    //    val v2 = Vectors.dense(0.3, 0.4, 0.5)

    val v1 = DenseVector(1, 2, 3, 4)
    val v2 = DenseVector(1, 1, 1, 1)

    val m1 = DenseMatrix((1.0, 2.0, 3.0), (4.0, 5.0, 6.0))
    val m2 = DenseMatrix((1.0, 1.0, 1.0), (2.0, 2.0, 2.0))

    val v3 = v1.dot(v2)
    println(v3)

    println(v1.norm())
    println(norm(v1))
    //    val m4 = norm(m1)
    //    println(m4)

    //cosineSimilarity:
    val t1 = (v1.dot(v2)) / (norm(v1) * norm(v2))
    println(t1)


    sc.stop()
    spark.stop()
  }
}
