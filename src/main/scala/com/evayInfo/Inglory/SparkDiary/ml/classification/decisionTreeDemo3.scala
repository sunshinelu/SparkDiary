package com.evayInfo.Inglory.SparkDiary.ml.classification

import com.evayInfo.Inglory.SparkDiary.ml.classification.SpamMessageClassifier.SetLogger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, Word2Vec}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object decisionTreeDemo3 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {
    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"SpamMessageClassifier").setMaster("local[*]").set("spark.executor.memory", "4g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val filePath = "file:///D:/Workspace/IDEA/GitHub/SparkDiary/data/smsspamcollection/SMSSpamCollection"

    val messageRDD = sc.textFile(filePath).map(_.split("\t")).map(line => {(line(0), line(1).split(" "))})

    val smsDF = spark.createDataFrame(messageRDD).toDF("labelCol", "contextCol")

    // 将原始的文本标签转换成数值型的表型
    val labelIndexer = new StringIndexer().
      setInputCol("labelCol").
      setOutputCol("indexedLabelCol").
      fit(smsDF)
    val labelIndexerDF = labelIndexer.transform(smsDF)

    // 使用word2vec将文本转化为数值型词向量
    val word2vec = new Word2Vec().
      setInputCol("contextCol").
      setOutputCol("featuresCol").
      setVectorSize(100).
      setMaxIter(1).fit(labelIndexerDF)
    val toVecUDF  = udf((arg:Double) => Vectors.dense(Array(arg)))
    val toVecUDF2  = udf((arg:Double) => Vectors.dense(arg))
    val word2vecDF = word2vec.transform(labelIndexerDF)//.withColumn("indexedLabelCol", toVecUDF($"indexedLabelCol"))
    word2vecDF.printSchema()
  word2vecDF.show()

    val test = word2vecDF.select("indexedLabelCol", "featuresCol")
    val layers = Array[Int](100, 6,5,2)

    // 使用 MultilayerPerceptronClassifier 训练一个多层感知器模型
    val mpc = new MultilayerPerceptronClassifier().
      setLayers(layers).
      setBlockSize(512).
      setSeed(1234L).
      setMaxIter(128).
      setFeaturesCol("indexedLabelCol").
      setPredictionCol("predictionCol")
      val mpcModel = mpc.fit(test)
    val mpcDF = mpcModel.transform(test)

    // 使用 LabelConverter 将预测结果的数值标签转换成原始的文本标签
    val labelConverter = new IndexToString().
      setInputCol("predictionCol").
      setOutputCol("predictedLabelCol").
      setLabels(labelIndexer.labels)
    val labelConverterDF = labelConverter.transform(mpcDF)
    labelConverterDF.printSchema()


    // 将原始文本数据按照 8:2 的比例分成训练集和测试集
//    val Array(trainingData, testData) = smsDF.randomSplit(Array(0.8, 0.2))

    sc.stop()
    spark.stop()
  }

}
