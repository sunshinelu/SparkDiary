package com.evayInfo.Inglory.SparkDiary.sparkSQL.UDF

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

/*
 * Created by sunlu on 17/7/29
 * 测试UDF
 * 使用def定义函数的时候必须有参数才能在udf中使用。
 */
object udfTest {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("udfTest").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val rdd1 = sc.parallelize(Seq((1,"a"),(2,"b"),(3,"c")))

    val df1 = rdd1.toDF("col1", "col2")
    df1.show()


    val uuidUDF = udf((arg:String) => func1(arg))
    val df2 = df1.withColumn("col3",uuidUDF($"col1"))
    df2.show()

    val uuidUDF2 = udf(func2())
    val df3 = df1.withColumn("col3",uuidUDF2())
    df3.show()

  }
  def func1(arg:String):String = {
    "FuncTest1"
  }

  def func2():String = {
    "FuncTest2"
  }

}
