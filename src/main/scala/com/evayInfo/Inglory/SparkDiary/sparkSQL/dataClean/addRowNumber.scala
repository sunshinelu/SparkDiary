package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 18/10/24.
 * demo描述：
 * 根据dataframe中的一列进行排序后，添加序号列
 *
 * 参考链接：
 * spark dataframe新增一列的四种方法
 * https://blog.csdn.net/li3xiao3jie2/article/details/81317249?utm_source=blogxgwz6
 *
 */
object addRowNumber {

  // 是否输出日志
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"addRowNumber").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df1 = spark.createDataFrame(Seq(("a",1),("a",2),("a",3),("b",1),("b",2))).toDF("col1","col2")
    df1.show(false)
    /*
+----+----+
|col1|col2|
+----+----+
|a   |1   |
|a   |2   |
|a   |3   |
|b   |1   |
|b   |2   |
+----+----+
     */

    val df2 = df1.orderBy($"col2".asc)withColumn("row_number",monotonically_increasing_id)
    df2.show(truncate = false)
    /*
+----+----+----------+
|col1|col2|row_number|
+----+----+----------+
|b   |1   |0         |
|a   |1   |1         |
|b   |2   |2         |
|a   |2   |3         |
|a   |3   |4         |
+----+----+----------+
     */

    sc.stop()
    spark.stop()

  }
}
