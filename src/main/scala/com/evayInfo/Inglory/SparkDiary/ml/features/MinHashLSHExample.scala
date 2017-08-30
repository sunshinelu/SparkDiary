package com.evayInfo.Inglory.SparkDiary.ml.features

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/8/30.
 */
object MinHashLSHExample {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    // bulid spark environment
    val sparkConf = new SparkConf().setAppName(s"MinHashLSHExample").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    // $example on$
    val dfA = spark.createDataFrame(Seq(
      (0, Vectors.sparse(6, Seq((0, 1.0), (1, 1.0), (2, 1.0)))),
      (1, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (4, 1.0)))),
      (2, Vectors.sparse(6, Seq((0, 1.0), (2, 1.0), (4, 1.0))))
    )).toDF("id", "keys")
    dfA.printSchema()
    /*
    root
     |-- id: integer (nullable = false)
     |-- keys: vector (nullable = true)
     */
    val dfB = spark.createDataFrame(Seq(
      (3, Vectors.sparse(6, Seq((1, 1.0), (3, 1.0), (5, 1.0)))),
      (4, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (5, 1.0)))),
      (5, Vectors.sparse(6, Seq((1, 1.0), (2, 1.0), (4, 1.0))))
    )).toDF("id", "keys")

    val key = Vectors.sparse(6, Seq((1, 1.0), (3, 1.0)))

    val mh = new MinHashLSH()
      .setNumHashTables(3)
      .setInputCol("keys")
      .setOutputCol("values")

    val model = mh.fit(dfA)

    // Feature Transformation
    model.transform(dfA).show()
    // Cache the transformed columns
    val transformedA = model.transform(dfA).cache()
    val transformedB = model.transform(dfB).cache()
    transformedA.printSchema()
    /*
    root
 |-- id: integer (nullable = false)
 |-- keys: vector (nullable = true)
 |-- values: array (nullable = true)
 |    |-- element: vector (containsNull = true)
     */
    transformedB.printSchema()

    // Approximate similarity join
    model.approxSimilarityJoin(dfA, dfB, 0.6).show()
    model.approxSimilarityJoin(transformedA, transformedB, 0.6).show()
    // Self Join
    model.approxSimilarityJoin(dfA, dfA, 0.6).filter("datasetA.id < datasetB.id").show()

    val df1 = model.approxSimilarityJoin(dfA, dfA, 0.6)
    println("df1 is: ")
    df1.printSchema()
    /*
    root
 |-- datasetA: struct (nullable = false)
 |    |-- id: integer (nullable = false)
 |    |-- keys: vector (nullable = true)
 |    |-- values: array (nullable = true)
 |    |    |-- element: vector (containsNull = true)
 |-- datasetB: struct (nullable = false)
 |    |-- id: integer (nullable = false)
 |    |-- keys: vector (nullable = true)
 |    |-- values: array (nullable = true)
 |    |    |-- element: vector (containsNull = true)
 |-- distCol: double (nullable = true)
     */
    df1.take(10000) foreach (println)
    /*
[[1,(6,[2,3,4],[1.0,1.0,1.0]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-4.86208737E8])],[1,(6,[2,3,4],[1.0,1.0,1.0]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-4.86208737E8])],0.0]
[[2,(6,[0,2,4],[1.0,1.0,1.0]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])],[0,(6,[0,1,2],[1.0,1.0,1.0]),WrappedArray([-2.031299587E9], [-1.974869772E9], [-1.974047307E9])],0.5]
[[0,(6,[0,1,2],[1.0,1.0,1.0]),WrappedArray([-2.031299587E9], [-1.974869772E9], [-1.974047307E9])],[2,(6,[0,2,4],[1.0,1.0,1.0]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])],0.5]
[[1,(6,[2,3,4],[1.0,1.0,1.0]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-4.86208737E8])],[2,(6,[0,2,4],[1.0,1.0,1.0]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])],0.5]
[[2,(6,[0,2,4],[1.0,1.0,1.0]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])],[2,(6,[0,2,4],[1.0,1.0,1.0]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])],0.0]
[[0,(6,[0,1,2],[1.0,1.0,1.0]),WrappedArray([-2.031299587E9], [-1.974869772E9], [-1.974047307E9])],[0,(6,[0,1,2],[1.0,1.0,1.0]),WrappedArray([-2.031299587E9], [-1.974869772E9], [-1.974047307E9])],0.0]
[[2,(6,[0,2,4],[1.0,1.0,1.0]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])],[1,(6,[2,3,4],[1.0,1.0,1.0]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-4.86208737E8])],0.5]

     */

    // Approximate nearest neighbor search
    model.approxNearestNeighbors(dfA, key, 2).show()
    model.approxNearestNeighbors(transformedA, key, 2).show()
    // $example off$


    sc.stop()
    spark.stop()
  }
}
