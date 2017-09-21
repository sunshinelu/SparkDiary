package com.evayInfo.Inglory.SparkDiary.sparkSQL.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 17/9/21.
 * 参考链接：https://stackoverflow.com/questions/33866759/spark-scala-dataframe-create-feature-vectors
 * 将dataframe转成Vectors
 */
object DataframeToVectors {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    val SparkConf = new SparkConf().setAppName(s"DataframeToVectors").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    /*
    1. Method one
     */
    val df = Seq((1, "cat1", 1), (1, "cat2", 3), (1, "cat9", 5),
      (2, "cat4", 6), (2, "cat9", 2), (2, "cat10", 1),
      (3, "cat1", 5), (3, "cat7", 16), (3, "cat8", 2))
      .toDF("userID", "category", "frequency")
    df.show
    /*
+------+--------+---------+
|userID|category|frequency|
+------+--------+---------+
|     1|    cat1|        1|
|     1|    cat2|        3|
|     1|    cat9|        5|
|     2|    cat4|        6|
|     2|    cat9|        2|
|     2|   cat10|        1|
|     3|    cat1|        5|
|     3|    cat7|       16|
|     3|    cat8|        2|
+------+--------+---------+
     */

    val pivoted = df.groupBy("userID").pivot("category").avg("frequency")
    val dfZeros = pivoted.na.fill(0)
    dfZeros.show
    /*
+------+----+-----+----+----+----+----+----+
|userID|cat1|cat10|cat2|cat4|cat7|cat8|cat9|
+------+----+-----+----+----+----+----+----+
|     1| 1.0|  0.0| 3.0| 0.0| 0.0| 0.0| 5.0|
|     3| 5.0|  0.0| 0.0| 0.0|16.0| 2.0| 0.0|
|     2| 0.0|  1.0| 0.0| 6.0| 0.0| 0.0| 2.0|
+------+----+-----+----+----+----+----+----+
     */


    def toSparseVectorUdf(size: Int) = udf {
      (data: Seq[Row]) => {
        val indices = data.map(_.getDouble(0).toInt).toArray
        val values = data.map(_.getInt(1).toDouble).toArray
        Vectors.sparse(size, indices, values)
      }
    }

    val indexer = new StringIndexer().setInputCol("category").setOutputCol("idx")
    val indexerModel = indexer.fit(df)
    val totalCategories = indexerModel.labels.size
    val dataWithIndices = indexerModel.transform(df)
    val data = dataWithIndices.groupBy("userId").agg(sort_array(collect_list(struct($"idx", $"frequency".as("val")))).as("data"))
    val dataWithFeatures = data.withColumn("features", toSparseVectorUdf(totalCategories)($"data")).drop("data")
    dataWithFeatures.show(false)
    /*
+------+--------------------------+
|userId|features                  |
+------+--------------------------+
|1     |(7,[0,1,3],[1.0,5.0,3.0]) |
|3     |(7,[0,2,4],[5.0,16.0,2.0])|
|2     |(7,[1,5,6],[2.0,6.0,1.0]) |
+------+--------------------------+
     */

    sc.stop()
    spark.stop()
  }
}
