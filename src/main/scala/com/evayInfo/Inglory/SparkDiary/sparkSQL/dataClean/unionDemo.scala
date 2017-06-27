package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/6/27.
 * 测试spark dataframe中的union函数
 */
object unionDemo {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }

  def main(args: Array[String]) {
    SetLogger
    val spark = SparkSession.builder.appName("unionDemo").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df1 = sc.parallelize(Seq((1, "a", "c"), (2, "d", "f"), (3, "r", "e"))).toDF("x1", "x2", "x3")

    val df2 = sc.parallelize(Seq((4, "a", "c"), (5, "d", "f"), (6, "r", "e"))).toDF("x1", "x2", "x3")

    val df3 = df1.union(df2)

    df3.show()
    /*
    +---+---+---+
| x1| x2| x3|
+---+---+---+
|  1|  a|  c|
|  2|  d|  f|
|  3|  r|  e|
|  4|  a|  c|
|  5|  d|  f|
|  6|  r|  e|
+---+---+---+
     */

  }

}
