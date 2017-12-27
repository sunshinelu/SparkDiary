package com.evayInfo.Inglory.SparkDiary.ml.classification

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{Word2Vec, StringIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/12/27.
 * 对情感数据构建多层感知器分类模型，并进行参数优化
 */
object mpcDemo4 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"mpcDemo4").setMaster("local[*]").set("spark.executor.memory", "4g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    // load stop words
    val stopwordsFile: String = "file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/Stopwords.dic"
    val stopwords = sc.textFile(stopwordsFile).collect().toList


    //connect mysql database and get sentiment data
    val url1 = "jdbc:mysql://localhost:3306/sunluMySQL"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")
    //get data
    val df = spark.read.jdbc(url1, "sentiment_test", prop1)

    val labelIndexer = new StringIndexer()
      .setInputCol("sentiLable")
      .setOutputCol("label")
      .fit(df)
    val labelDF = labelIndexer.transform(df)
    // seg words
    val SegwordsUDF = udf((content:String) => content.split(" ").map(_.split("/")(0)).filter(x => ! stopwords.contains(x)).toSeq)
    val segDF = labelDF.withColumn("seg", SegwordsUDF($"content"))


    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("seg")
      .setOutputCol("features")
      .setVectorSize(100)
      .setMinCount(1)
    val word2vecModel = word2Vec.fit(segDF)
    val word2vecDF = word2vecModel.transform(segDF)

    val Array(trainingDF, testDF) = word2vecDF.randomSplit(Array(0.8, 0.2), 1234L)

    val layers = Array(20, 8, 6,5, 4)
//    val maxIterArray = Array[Int](5, 10, 20, 50, 100, 200)
    val maxIterArray = Array[Int](200, 300, 400, 500, 600)
    val stepSizeArray = Array[Double](0.01, 0.02, 0.03, 0.04, 0.05)
    val tolArray = Array[Double](1e-4, 1e-5, 1e-6, 1e-7, 1e-8)

    val evaluations = for(maxIter <- maxIterArray; stepSize <- stepSizeArray; tol <- tolArray) yield {
      val multilayerPerceptronClassifier = new MultilayerPerceptronClassifier().
        setLabelCol("label").
        setFeaturesCol("features").
        setLayers(layers).
        setMaxIter(maxIter).
        setStepSize(stepSize).
        setTol(tol).
        setBlockSize(128).
        setSeed(1000)
      val model = multilayerPerceptronClassifier.fit(trainingDF)

      val predictions = model.transform(testDF)
      val evaluator = new MulticlassClassificationEvaluator().
        setLabelCol("label").
        setPredictionCol("prediction").
        setMetricName("accuracy")
      val accuracy = evaluator.evaluate(predictions)
      println("maxIter = " + maxIter + "; stepSize = " + stepSize + "; tol = " + tol + " ; accuracy = " + accuracy)
      (maxIter, stepSize, tol, accuracy)
    }

    evaluations.foreach(println)
    /*
(200,0.01,1.0E-4,0.2923076923076923)
(200,0.01,1.0E-5,0.2923076923076923)
(200,0.01,1.0E-6,0.2923076923076923)
(200,0.01,1.0E-7,0.2923076923076923)
(200,0.01,1.0E-8,0.2923076923076923)
(200,0.02,1.0E-4,0.2923076923076923)
(200,0.02,1.0E-5,0.2923076923076923)
(200,0.02,1.0E-6,0.2923076923076923)
(200,0.02,1.0E-7,0.2923076923076923)
(200,0.02,1.0E-8,0.2923076923076923)
(200,0.03,1.0E-4,0.2923076923076923)
(200,0.03,1.0E-5,0.2923076923076923)
(200,0.03,1.0E-6,0.2923076923076923)
(200,0.03,1.0E-7,0.2923076923076923)
(200,0.03,1.0E-8,0.2923076923076923)
(200,0.04,1.0E-4,0.2923076923076923)
(200,0.04,1.0E-5,0.2923076923076923)
(200,0.04,1.0E-6,0.2923076923076923)
(200,0.04,1.0E-7,0.2923076923076923)
(200,0.04,1.0E-8,0.2923076923076923)
(200,0.05,1.0E-4,0.2923076923076923)
(200,0.05,1.0E-5,0.2923076923076923)
(200,0.05,1.0E-6,0.2923076923076923)
(200,0.05,1.0E-7,0.2923076923076923)
(200,0.05,1.0E-8,0.2923076923076923)
(300,0.01,1.0E-4,0.3076923076923077)
(300,0.01,1.0E-5,0.3076923076923077)
(300,0.01,1.0E-6,0.3076923076923077)
(300,0.01,1.0E-7,0.3076923076923077)
(300,0.01,1.0E-8,0.3076923076923077)
(300,0.02,1.0E-4,0.3076923076923077)
(300,0.02,1.0E-5,0.3076923076923077)
(300,0.02,1.0E-6,0.3076923076923077)
(300,0.02,1.0E-7,0.3076923076923077)
(300,0.02,1.0E-8,0.3076923076923077)
(300,0.03,1.0E-4,0.3076923076923077)
(300,0.03,1.0E-5,0.3076923076923077)
(300,0.03,1.0E-6,0.3076923076923077)
(300,0.03,1.0E-7,0.3076923076923077)
(300,0.03,1.0E-8,0.3076923076923077)
(300,0.04,1.0E-4,0.3076923076923077)
(300,0.04,1.0E-5,0.3076923076923077)
(300,0.04,1.0E-6,0.3076923076923077)
(300,0.04,1.0E-7,0.3076923076923077)
(300,0.04,1.0E-8,0.3076923076923077)
(300,0.05,1.0E-4,0.3076923076923077)
(300,0.05,1.0E-5,0.3076923076923077)
(300,0.05,1.0E-6,0.3076923076923077)
(300,0.05,1.0E-7,0.3076923076923077)
(300,0.05,1.0E-8,0.3076923076923077)
(400,0.01,1.0E-4,0.3230769230769231)
(400,0.01,1.0E-5,0.3230769230769231)
(400,0.01,1.0E-6,0.3230769230769231)
(400,0.01,1.0E-7,0.3230769230769231)
(400,0.01,1.0E-8,0.3230769230769231)
(400,0.02,1.0E-4,0.3230769230769231)
(400,0.02,1.0E-5,0.3230769230769231)
(400,0.02,1.0E-6,0.3230769230769231)
(400,0.02,1.0E-7,0.3230769230769231)
(400,0.02,1.0E-8,0.3230769230769231)
(400,0.03,1.0E-4,0.3230769230769231)
(400,0.03,1.0E-5,0.3230769230769231)
(400,0.03,1.0E-6,0.3230769230769231)
(400,0.03,1.0E-7,0.3230769230769231)
(400,0.03,1.0E-8,0.3230769230769231)
(400,0.04,1.0E-4,0.3230769230769231)
(400,0.04,1.0E-5,0.3230769230769231)
(400,0.04,1.0E-6,0.3230769230769231)
(400,0.04,1.0E-7,0.3230769230769231)
(400,0.04,1.0E-8,0.3230769230769231)
(400,0.05,1.0E-4,0.3230769230769231)
(400,0.05,1.0E-5,0.3230769230769231)
(400,0.05,1.0E-6,0.3230769230769231)
(400,0.05,1.0E-7,0.3230769230769231)
(400,0.05,1.0E-8,0.3230769230769231)
(500,0.01,1.0E-4,0.3384615384615385)
(500,0.01,1.0E-5,0.3384615384615385)
(500,0.01,1.0E-6,0.3384615384615385)
(500,0.01,1.0E-7,0.3384615384615385)
(500,0.01,1.0E-8,0.3384615384615385)
(500,0.02,1.0E-4,0.3384615384615385)
(500,0.02,1.0E-5,0.3384615384615385)
(500,0.02,1.0E-6,0.3384615384615385)
(500,0.02,1.0E-7,0.3384615384615385)
(500,0.02,1.0E-8,0.3384615384615385)
(500,0.03,1.0E-4,0.3384615384615385)
(500,0.03,1.0E-5,0.3384615384615385)
(500,0.03,1.0E-6,0.3384615384615385)
(500,0.03,1.0E-7,0.3384615384615385)
(500,0.03,1.0E-8,0.3384615384615385)
(500,0.04,1.0E-4,0.3384615384615385)
(500,0.04,1.0E-5,0.3384615384615385)
(500,0.04,1.0E-6,0.3384615384615385)
(500,0.04,1.0E-7,0.3384615384615385)
(500,0.04,1.0E-8,0.3384615384615385)
(500,0.05,1.0E-4,0.3384615384615385)
(500,0.05,1.0E-5,0.3384615384615385)
(500,0.05,1.0E-6,0.3384615384615385)
(500,0.05,1.0E-7,0.3384615384615385)
(500,0.05,1.0E-8,0.3384615384615385)
(600,0.01,1.0E-4,0.3384615384615385)
(600,0.01,1.0E-5,0.3384615384615385)
(600,0.01,1.0E-6,0.3384615384615385)
(600,0.01,1.0E-7,0.3384615384615385)
(600,0.01,1.0E-8,0.3384615384615385)
(600,0.02,1.0E-4,0.3384615384615385)
(600,0.02,1.0E-5,0.3384615384615385)
(600,0.02,1.0E-6,0.3384615384615385)
(600,0.02,1.0E-7,0.3384615384615385)
(600,0.02,1.0E-8,0.3384615384615385)
(600,0.03,1.0E-4,0.3384615384615385)
(600,0.03,1.0E-5,0.3384615384615385)
(600,0.03,1.0E-6,0.3384615384615385)
(600,0.03,1.0E-7,0.3384615384615385)
(600,0.03,1.0E-8,0.3384615384615385)
(600,0.04,1.0E-4,0.3384615384615385)
(600,0.04,1.0E-5,0.3384615384615385)
(600,0.04,1.0E-6,0.3384615384615385)
(600,0.04,1.0E-7,0.3384615384615385)
(600,0.04,1.0E-8,0.3384615384615385)
(600,0.05,1.0E-4,0.3384615384615385)
(600,0.05,1.0E-5,0.3384615384615385)
(600,0.05,1.0E-6,0.3384615384615385)
(600,0.05,1.0E-7,0.3384615384615385)
(600,0.05,1.0E-8,0.3384615384615385)
     */


    sc.stop()
    spark.stop()
  }

}
