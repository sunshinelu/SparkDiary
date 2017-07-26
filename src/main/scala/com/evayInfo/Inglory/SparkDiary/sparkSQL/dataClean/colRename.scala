package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/7/19.
 * 对spark中的dataset和dataframe中的列重命名
 */
object colRename {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }

  case class colName1(col1: Int, col2: String, col3: String)

  def main(args: Array[String]) {
    SetLogger
    val spark = SparkSession.builder.appName("colRename").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val rdd = sc.parallelize(Seq(colName1(1, "a", "c"), colName1(2, "d", "f"), colName1(3, "r", "e")))

    val ds1 = spark.createDataset(rdd) //.as[colName1]
    ds1.printSchema()

    /*
    对dataset进行rename操作时，需要将dataset转为dataframe
     */
    val ds1ColumnsName = Seq("id", "s1", "s2")
    val ds2 = ds1.as[(Int, String, String)].toDF(ds1ColumnsName: _*)
    ds2.printSchema()

    val df1 = spark.createDataFrame(rdd) //.as[colName1]
    df1.printSchema()

    val df2 = df1.toDF(ds1ColumnsName: _*)
    df2.printSchema()


  }

}
