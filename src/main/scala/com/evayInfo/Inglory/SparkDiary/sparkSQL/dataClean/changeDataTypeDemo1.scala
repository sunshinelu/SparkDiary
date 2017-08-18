package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/8/18.
 *
 * 更改数据框的数据类型
 */
object changeDataTypeDemo1 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger
    val conf = new SparkConf().setAppName(s"changeDataTypeDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3))).toDF("col1", "col2")
    df.printSchema()
    /*
    root
 |-- col1: string (nullable = true)
 |-- col2: integer (nullable = true)
     */
    df.show()

    val df1 = df.withColumn("col2", df("col2").cast("string"))
    df1.printSchema()
    /*
    root
 |-- col1: string (nullable = true)
 |-- col2: string (nullable = true)
     */
    df1.show()


    val df2 = df.withColumn("col2", df("col2").cast("double"))
    df2.printSchema()
    /*
    root
 |-- col1: string (nullable = true)
 |-- col2: double (nullable = true)
     */
    df2.show()


    val df3 = df.withColumn("col2", df("col2").cast("long"))
    df3.printSchema()
    /*
    root
 |-- col1: string (nullable = true)
 |-- col2: long (nullable = true)
     */
    df3.show()

    sc.stop()
    spark.stop()
  }
}
