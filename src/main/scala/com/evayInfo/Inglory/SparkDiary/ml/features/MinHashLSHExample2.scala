package com.evayInfo.Inglory.SparkDiary.ml.features

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/9/29.
 */
object MinHashLSHExample2 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    // bulid spark environment
    val sparkConf = new SparkConf().setAppName(s"MinHashLSHExample2").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    // $example on$
    val df = spark.createDataFrame(Seq(
      (0, Vectors.sparse(6, Seq((0, 1.0), (1, 1.0), (2, 1.0)))),
      (1, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (4, 1.0)))),
      (2, Vectors.sparse(6, Seq((0, 1.0), (2, 1.0), (4, 1.0)))),
      (3, Vectors.sparse(6, Seq((1, 1.0), (3, 1.0), (5, 1.0)))),
      (4, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (5, 1.0)))),
      (5, Vectors.sparse(6, Seq((1, 1.0), (2, 1.0), (4, 1.0))))
    )).toDF("id", "keys")
    df.printSchema()
    /*
    root
     |-- id: integer (nullable = false)
     |-- keys: vector (nullable = true)
     */

    df.show(false)
    /*
+---+-------------------------+
|id |keys                     |
+---+-------------------------+
|0  |(6,[0,1,2],[1.0,1.0,1.0])|
|1  |(6,[2,3,4],[1.0,1.0,1.0])|
|2  |(6,[0,2,4],[1.0,1.0,1.0])|
|3  |(6,[1,3,5],[1.0,1.0,1.0])|
|4  |(6,[2,3,5],[1.0,1.0,1.0])|
|5  |(6,[1,2,4],[1.0,1.0,1.0])|
+---+-------------------------+
     */

    val key = Vectors.sparse(6, Seq((1, 1.0), (3, 1.0)))

    val mh = new MinHashLSH()
      .setNumHashTables(3)
      .setInputCol("keys")
      .setOutputCol("values")

    val model = mh.fit(df)



    val df1 = model.approxSimilarityJoin(df, df, 1.0).select("datasetA.id", "datasetB.id", "distCol")
    df1.show(false)
    /*
    +---+---+-------+
    |id |id |distCol|
    +---+---+-------+
    |3  |5  |0.8    |
    |0  |5  |0.5    |
    |1  |1  |0.0    |
    |3  |0  |0.8    |
    |2  |0  |0.5    |
    |5  |4  |0.8    |
    |0  |4  |0.8    |
    |0  |1  |0.8    |
    |4  |1  |0.5    |
    |2  |4  |0.8    |
    |1  |0  |0.8    |
    |1  |4  |0.5    |
    |4  |2  |0.8    |
    |1  |5  |0.5    |
    |5  |5  |0.0    |
    |0  |2  |0.5    |
    |2  |5  |0.5    |
    |4  |4  |0.0    |
    |0  |3  |0.8    |
    |1  |2  |0.5    |
    +---+---+-------+
     */
    val df2 = model.approxNearestNeighbors(df, key, 5)
    df2.show(false)
    /*
    +---+-------------------------+------------------------------------------------------+-------------------+
    |id |keys                     |values                                                |distCol            |
    +---+-------------------------+------------------------------------------------------+-------------------+
    |3  |(6,[1,3,5],[1.0,1.0,1.0])|[[-1.278435698E9], [-1.974869772E9], [-1.230128022E9]]|0.33333333333333337|
    |5  |(6,[1,2,4],[1.0,1.0,1.0])|[[-2.031299587E9], [-1.974869772E9], [-1.230128022E9]]|0.75               |
    |0  |(6,[0,1,2],[1.0,1.0,1.0])|[[-2.031299587E9], [-1.974869772E9], [-1.974047307E9]]|0.75               |
    +---+-------------------------+------------------------------------------------------+-------------------+

     */
    val df_filter = df.filter($"id" >= 3)
    val df3 = model.approxSimilarityJoin(df_filter, df, 1.0).select("datasetA.id", "datasetB.id", "distCol")
    df3.show(false)
    /*
    +---+---+-------+
    |id |id |distCol|
    +---+---+-------+
    |3  |5  |0.8    |
    |3  |0  |0.8    |
    |5  |4  |0.8    |
    |4  |1  |0.5    |
    |4  |2  |0.8    |
    |5  |5  |0.0    |
    |4  |4  |0.0    |
    |5  |1  |0.5    |
    |4  |0  |0.8    |
    |5  |2  |0.5    |
    |5  |3  |0.8    |
    |4  |5  |0.8    |
    |5  |0  |0.5    |
    |3  |3  |0.0    |
    +---+---+-------+
     */
    val df4 = model.approxSimilarityJoin(df, df_filter, 1.0).select("datasetA.id", "datasetB.id", "distCol")
    df4.toDF("id1", "id2", "dist").orderBy($"id2", $"id1".asc) show (false)
    /*
+---+---+----+
|id1|id2|dist|
+---+---+----+
|0  |3  |0.8 |
|3  |3  |0.0 |
|5  |3  |0.8 |
|0  |4  |0.8 |
|1  |4  |0.5 |
|2  |4  |0.8 |
|4  |4  |0.0 |
|5  |4  |0.8 |
|0  |5  |0.5 |
|1  |5  |0.5 |
|2  |5  |0.5 |
|3  |5  |0.8 |
|4  |5  |0.8 |
|5  |5  |0.0 |
+---+---+----+
     */
    sc.stop()
    spark.stop()
  }

}
