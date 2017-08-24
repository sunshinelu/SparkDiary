package com.evayInfo.Inglory.Project.DocsSimilarity

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, MatrixEntry}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 17/8/23.
 */
object DocsimiALS {

  def main(args: Array[String]): Unit = {
    DocsimiUtil.SetLogger

    val sparkConf = new SparkConf().setAppName(s"DocsimiALS").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val ylzxTable = "yilan-total_webpage"//args(0)
    val logsTable = "t_hbaseSink"//args(1)
    val outputTable = "docsimi_als"//args(2)

    val ylzxRDD = DocsimiUtil.getYlzxRDD(ylzxTable, sc)
    val ylzxDF = spark.createDataset(ylzxRDD).dropDuplicates("content").drop("content")

    val logsRDD = DocsimiUtil.getLogsRDD(logsTable, sc)
    val logsDS = spark.createDataset(logsRDD).na.drop(Array("userString"))
    val ds1 = logsDS.groupBy("userString", "itemString").agg(sum("value")).withColumnRenamed("sum(value)", "rating")
    //string to number
    val userID = new StringIndexer().setInputCol("userString").setOutputCol("userID").fit(ds1)
    val ds2 = userID.transform(ds1)
    val urlID = new StringIndexer().setInputCol("itemString").setOutputCol("urlID").fit(ds2)
    val ds3 = urlID.transform(ds2)


    //RDD to RowRDD
    val alsRDD = ds3.select("userID", "urlID", "norm").rdd.
      map { case Row(userID: Double, urlID: Double, rating: Double) =>
        Rating(userID.toInt, urlID.toInt, rating)
      }
    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10
    val alsModel = ALS.train(alsRDD, rank, numIterations, 0.01)

    val userFeatures = alsModel.productFeatures.map {
      case (id: Int, vec: Array[Double]) => {
        val vector = Vectors.dense(vec)
        new IndexedRow(id, vector)
      }
    }

    /* The minimum cosine similarity threshold for each document pair */
    val threshhold = 0.5.toDouble
    val upper = 1.0

    val mat = new IndexedRowMatrix(userFeatures)
    val mat_t = mat.toCoordinateMatrix.transpose()
    val sim = mat_t.toRowMatrix.columnSimilarities(threshhold)
    val sim2 = sim.entries.map { case MatrixEntry(i, j, u) => (i, j, u) }

    val sim_threshhold = sim.entries.filter { case MatrixEntry(i, j, u) => u >= threshhold && u <= upper }
    println("item-item similarity is: ")
    sim_threshhold.collect().foreach(println)

    val sim_threshhold2 = sim_threshhold.map { case MatrixEntry(i, j, u) => MatrixEntry(j, i, u) }
    val docsim = sim_threshhold.union(sim_threshhold2)



    sc.stop()
    spark.stop()
  }
}
