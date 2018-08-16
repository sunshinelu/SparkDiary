package com.evayInfo.Inglory.SparkDiary.Vector

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 18/8/16.
 *
 * 将Scala vector转换成ML vector
 *
 * How to convert scala vector to spark ML vector?
 * https://stackoverflow.com/questions/42431292/how-to-convert-scala-vector-to-spark-ml-vector
 *
 */
object ScalaVectorToMlVector {
  // do not print log
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger
    val conf = new SparkConf().setAppName(s"ScalaVectorToMlVector").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val test1 = Seq(1.0,2.0,3.0,4.0,5.0).to[scala.Vector].toArray
    val v1 = Vectors.dense(test1)



    val test2 = Seq(1,2,3,4,5).map(_.toDouble).to[scala.Vector].toArray
    val v2 = Vectors.dense(test2)

    val testDouble = Seq(1,2,3,4,5).map(x=>x.toDouble).to[scala.Vector].toArray
    Vectors.dense(testDouble)

    val sqdistTest = Vectors.sqdist(v1, v2)
    println("sqdistTest is: " + sqdistTest)

    sc.stop()
    spark.stop()
  }

}
