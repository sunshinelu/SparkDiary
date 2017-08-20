package com.evayInfo.Inglory.TestCode

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/**
 * Created by sunlu on 17/7/27.
 * 使用rt.jar生成UUID
 */
object uuidGenerate {
  def main(args: Array[String]) {
    val s1 = UUID.randomUUID().toString().toLowerCase()
    println(s1)

    val sparkConf = new SparkConf().setAppName("uuidGenerate").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val a = UUID.randomUUID().toString().toLowerCase()

    println(a.length)

    val rdd1 = sc.parallelize(Seq((1,"a"),(2,"b"),(3,"c")))

    val df1 = rdd1.toDF("col1", "col2")
    df1.show()


    val uuidUDF = udf((arg:String) => uuidFunc(arg))
    val df2 = df1.withColumn("col3",uuidUDF($"col1"))
    df2.show()

    val uuidUDF2 = udf(uuidFunc2())
    val df3 = df1.withColumn("col3",uuidUDF2())
    df3.show()

    sc.stop()
    spark.stop()
  }
  def uuidFunc(arg:String):String={
    val uuid = UUID.randomUUID().toString().toLowerCase()
    uuid
  }

  def uuidFunc2():String={
    val uuid = UUID.randomUUID().toString().toLowerCase().toString
    uuid
  }

}
