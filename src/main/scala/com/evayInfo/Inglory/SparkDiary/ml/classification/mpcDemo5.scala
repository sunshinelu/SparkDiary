package com.evayInfo.Inglory.SparkDiary.ml.classification

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, Word2Vec, StringIndexer}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/12/27.
 * 《图解Spark》第九章Spark MLlib（P338）例子重写，不使用pipeline
 * https://www.ibm.com/developerworks/cn/opensource/os-cn-spark-practice6/
 *
 */
object mpcDemo5 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"mpcDemo5").setMaster("local[*]").set("spark.executor.memory", "4g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val filePath = "file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/smsspamcollection/SMSSpamCollection"

    val messageRDD = sc.textFile(filePath).map(_.split("\t")).map(line => {(line(0), line(1).split(" "))})

    val smsDF = spark.createDataFrame(messageRDD).toDF("labelCol", "contextCol")

    val labelIndexer = new StringIndexer()
      .setInputCol("labelCol")
      .setOutputCol("label")
      .fit(smsDF)
    val labelDF = labelIndexer.transform(smsDF)

    val layers = Array[Int](100, 6,5,2)

    // 使用word2vec将文本转化为数值型词向量
    val word2vec = new Word2Vec().
      setInputCol("contextCol").
      setOutputCol("featuresCol").
      setVectorSize(100).
      setMaxIter(1)
    val word2vecModel = word2vec.fit(labelDF)
    val word2vecDF = word2vecModel.transform(labelDF)

    val Array(trainingDF, testDF) = word2vecDF.randomSplit(Array(0.8, 0.2), 1234L)

    // 使用 MultilayerPerceptronClassifier 训练一个多层感知器模型
    val mpc = new MultilayerPerceptronClassifier().
      setLayers(layers).
      setBlockSize(512).
      setSeed(1234L).
      setMaxIter(128).
      setLabelCol("label").
      setFeaturesCol("featuresCol").
      setPredictionCol("predictionCol")
    val mpcModel = mpc.fit(trainingDF)

    val predictions = mpcModel.transform(testDF)
    val evaluator = new MulticlassClassificationEvaluator().
      setLabelCol("label").
      setPredictionCol("predictionCol").
      setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("accuracy is: " + accuracy)

    // 使用 LabelConverter 将预测结果的数值标签转换成原始的文本标签
    val labelConverter = new IndexToString().
      setInputCol("predictionCol").
      setOutputCol("predictedLabelCol").
      setLabels(labelIndexer.labels)

    val predictionsLabel = labelConverter.transform(predictions)
    predictionsLabel.printSchema()
    predictionsLabel.show()

    sc.stop()
    spark.stop()
  }

}
