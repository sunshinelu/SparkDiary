package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/8/25.
 */
object colContansDemo1 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    val conf = new SparkConf().setAppName(s"colContansDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df = sc.parallelize(Seq(("A;B;C", "A"), ("A;B;D", "B"), ("A;B;D", "C"), ("A;B;C", "D"))).toDF("col1", "col2")
    df.printSchema()
    df.show()
    /*
    +-----+----+
    | col1|col2|
    +-----+----+
    |A;B;C|   A|
    |A;B;D|   B|
    |A;B;D|   C|
    |A;B;C|   D|
    +-----+----+
     */
    val df1 = df.filter($"col1".contains($"col2"))
    df1.printSchema()

    df1.show()
    /*
+-----+----+
| col1|col2|
+-----+----+
|A;B;C|   A|
|A;B;D|   B|
+-----+----+
     */

    sc.stop()
    spark.stop()
  }
}
