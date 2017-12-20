package com.evayInfo.Inglory.SparkDiary.ml.classification

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, Word2Vec}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/12/20.
 * 《图解Spark》第九章Spark MLlib（P338）
 * http://mangocool.com/1476675638181.html
 * https://stackoverflow.com/questions/38664972/why-is-unable-to-find-encoder-for-type-stored-in-a-dataset-when-creating-a-dat
 */
object SpamMessageClassifier {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  def main(args: Array[String]) {

    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"SpamMessageClassifier").setMaster("local[*]").set("spark.executor.memory", "4g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext

    val filePath = "file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/smsspamcollection/SMSSpamCollection"

    val messageRDD = sc.textFile(filePath).map(_.split("\t")).map(line => {(line(0), line(1).split(" "))})

    val smsDF = spark.createDataFrame(messageRDD).toDF("labelCol", "contextCol")

    // 将原始的文本标签转换成数值型的表型
    val labelIndexer = new StringIndexer().
      setInputCol("labelCol").
      setOutputCol("indexedLabelCol").
      fit(smsDF)

    // 使用word2vec将文本转化为数值型词向量
    val word2vec = new Word2Vec().setInputCol("contextCol").setOutputCol("featuresCol").setVectorSize(100).setMaxIter(1)

    val layers = Array[Int](100, 6,5,2)

    // 使用 MultilayerPerceptronClassifier 训练一个多层感知器模型
    val mpc = new MultilayerPerceptronClassifier().
      setLayers(layers).
      setBlockSize(512).
      setSeed(1234L).
      setMaxIter(128).
      setFeaturesCol("indexedLabelCol").
      setPredictionCol("predictionCol")

    // 使用 LabelConverter 将预测结果的数值标签转换成原始的文本标签
    val labelConverter = new IndexToString().
      setInputCol("predictionCol").
      setOutputCol("predictedLabelCol").
      setLabels(labelIndexer.labels)

    // 将原始文本数据按照 8:2 的比例分成训练集和测试集
    val Array(trainingData, testData) = smsDF.randomSplit(Array(0.8, 0.2))

    // 使用 pipeline 对数据进行处理和模型训练
    val pipeline  = new Pipeline().setStages(Array(labelIndexer, word2vec, mpc, labelConverter))
    val model = pipeline.fit(trainingData)
    val preResultDF = model.transform(testData)

    // 使用模型对预测数据进行分类处理并在屏幕上打印20笔数据
    preResultDF.select("contextCol", "labelCol", "predictedLabelCol").show(20)

    // 测试数据集上测试模型的预测精确度
    val evaluator = new MulticlassClassificationEvaluator().
      setLabelCol("indexedLabelCol").
      setPredictionCol("predictionCol")

    val predictionAccuracy = evaluator.evaluate(preResultDF)

    println("Testing Accuracy is %2.4f".format(predictionAccuracy * 100) + "%")

    sc.stop()
    spark.stop()
  }
}
