package com.evayInfo.Inglory.SparkDiary.ml.classification

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, IDF, HashingTF}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/12/25.
 */
object NaiveBayesDemo1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  def main(args: Array[String]) {
    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"NaiveBayesDemo1").setMaster("local[*]").set("spark.executor.memory", "4g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // load sentiment datasets
    val label_0 = spark.read.format("text").load("file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/sentimentDic/weibo2/0_simplifyweibo.txt").
      toDF("content").withColumn("sentiLable", lit("喜悦")).limit(100)
    val label_1 = spark.read.format("text").load("file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/sentimentDic/weibo2/1_simplifyweibo.txt").
        toDF("content").withColumn("sentiLable", lit("愤怒")).limit(100)
    val label_2 = spark.read.format("text").load("file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/sentimentDic/weibo2/2_simplifyweibo.txt").
        toDF("content").withColumn("sentiLable", lit("厌恶")).limit(100)
    val label_3 = spark.read.format("text").load("file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/sentimentDic/weibo2/3_simplifyweibo.txt").
        toDF("content").withColumn("sentiLable", lit("低落")).limit(100)

    // load stop words
    val stopwordsFile: String = "file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/Stopwords.dic"
    val stopwords = sc.textFile(stopwordsFile).collect().toList


    val df = label_0.union(label_1).union(label_2).union(label_3)
//    df.coalesce(1).write.format("csv").mode(SaveMode.Overwrite).save("file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/sentimentDic/weiboTest.csv")

    val labelIndexer = new StringIndexer()
      .setInputCol("sentiLable")
      .setOutputCol("label")
      .fit(df)
    val labelDF = labelIndexer.transform(df)


    // seg words
    val SegwordsUDF = udf((content:String) => content.split(" ").map(_.split("/")(0)).filter(x => ! stopwords.contains(x)).toSeq)
    val segDF = labelDF.withColumn("seg", SegwordsUDF($"content"))

    val hashingTF = new HashingTF().
      setInputCol("seg").
      setOutputCol("rawFeatures").
      setNumFeatures(2000)

    val featurizedData = hashingTF.transform(segDF)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().
      setInputCol("rawFeatures").
      setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val tfidfData = idfModel.transform(featurizedData)
    tfidfData.printSchema()


    // split training set and tset set
    val Array(trainDF, testDF) = tfidfData.randomSplit(Array(0.8,0.2))

    val model = new NaiveBayes().
      setLabelCol("label").
      setFeaturesCol("features").
      fit(trainDF)
    // Select example rows to display.
    val predictions = model.transform(testDF)
    predictions.printSchema()
    predictions.show()

    // 使用 LabelConverter 将预测结果的数值标签转换成原始的文本标签
    val labelConverter = new IndexToString().
      setInputCol("prediction").
      setOutputCol("predictedLabelCol").
      setLabels(labelIndexer.labels)

    val predictionsLabel = labelConverter.transform(predictions)
    predictionsLabel.printSchema()
    predictionsLabel.show()


    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test set accuracy = " + accuracy)


    sc.stop()
    spark.stop()
  }

}
