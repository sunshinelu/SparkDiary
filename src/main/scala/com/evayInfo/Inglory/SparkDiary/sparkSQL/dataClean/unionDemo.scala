package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/6/27.
 * 测试spark dataframe中的union函数
 * 在使用union函数时，column的顺序必须一致
 *
 * 参考链接：https://stackoverflow.com/questions/39758045/how-to-union-2-spark-dataframes-with-different-amounts-of-columns
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


    val df4 = sc.parallelize(Seq((1, "a", "c"), (2, "d", "f"), (3, "r", "e"))).toDF("x1", "x3", "x2")
    val df5 = df1.union(df4)
    df5.show

    /*
    +---+---+---+
| x1| x2| x3|
+---+---+---+
|  1|  a|  c|
|  2|  d|  f|
|  3|  r|  e|
|  1|  a|  c|
|  2|  d|  f|
|  3|  r|  e|
+---+---+---+
     */
    val df6 = df1.unionAll(df4)
    df6.show()



    sc.stop()
    spark.stop()

  }

}
