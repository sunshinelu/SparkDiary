package com.evayInfo.Inglory.TestCode

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/8/20.
 */
object getUUID {
  def main(args: Array[String]) {
    val s1 = UUID.randomUUID().toString().toLowerCase()
    println(s1)

    val sparkConf = new SparkConf().setAppName("uuidGenerate").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val a = UUID.randomUUID().toString().toLowerCase()

    println(a.length)

    val b = a + "alkjdlkfjadljf"

    sc.stop()
    spark.stop()
  }
}
