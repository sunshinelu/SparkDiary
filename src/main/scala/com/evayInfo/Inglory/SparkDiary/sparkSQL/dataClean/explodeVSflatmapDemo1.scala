package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/8/17.
 * 参考链接：
 * https://stackoverflow.com/questions/36784735/how-to-flatmap-a-nested-dataframe-in-spark
 */
object explodeVSflatmapDemo1 {

  def main(args: Array[String]) {

    SetLogger

    val conf = new SparkConf().setAppName(s"explodeVSflatmapDemo").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df = Seq(("A", "B", "x,y,z", "D")).toDF("x1", "x2", "x3", "x4")
    df.show
    /*
+---+---+-----+---+
| x1| x2|   x3| x4|
+---+---+-----+---+
|  A|  B|x,y,z|  D|
+---+---+-----+---+
     */

    /*
    Spark 2.0+
    Dataset.flatMap:
     */
    val ds = df.as[(String, String, String, String)]
    val df1 = ds.flatMap {
      case (x1, x2, x3, x4) => x3.split(",").map((x1, x2, _, x4))
    }.toDF

    df1.show
    /*
+---+---+---+---+
| _1| _2| _3| _4|
+---+---+---+---+
|  A|  B|  x|  D|
|  A|  B|  y|  D|
|  A|  B|  z|  D|
+---+---+---+---+
     */

    /*
    Spark 1.3+
    Use split and explode functions:
     */
    val df2 = df.withColumn("x3", explode(split($"x3", ",")))
    df2.show
    /*
    +---+---+---+---+
    | x1| x2| x3| x4|
    +---+---+---+---+
    |  A|  B|  x|  D|
    |  A|  B|  y|  D|
    |  A|  B|  z|  D|
    +---+---+---+---+

     */
    /*
    Spark 1.x
    DataFrame.explode (deprecated in Spark 2.x)
     */
    val df3 = df.explode($"x3")(_.getAs[String](0).split(",").map(Tuple1(_)))
    df3.show
    /*
    +---+---+-----+---+---+
    | x1| x2|   x3| x4| _1|
    +---+---+-----+---+---+
    |  A|  B|x,y,z|  D|  x|
    |  A|  B|x,y,z|  D|  y|
    |  A|  B|x,y,z|  D|  z|
    +---+---+-----+---+---+

     */
    sc.stop()
    spark.stop()
  }

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }
}
