package com.evayInfo.Inglory.SparkDiary.ml.classification

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, Word2Vec, StringIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/12/27.
 * 对情感分析数据构建分类模型
 */
object mpcDemo3 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"mpcDemo3").setMaster("local[*]").set("spark.executor.memory", "4g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // load sentiment datasets
    val label_0 = spark.read.format("text").load("file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/sentimentDic/weibo2/0_simplifyweibo.txt").
      toDF("content").withColumn("sentiLable", lit("喜悦"))//.limit(100)
    val label_1 = spark.read.format("text").load("file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/sentimentDic/weibo2/1_simplifyweibo.txt").
      toDF("content").withColumn("sentiLable", lit("愤怒"))//.limit(100)
    val label_2 = spark.read.format("text").load("file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/sentimentDic/weibo2/2_simplifyweibo.txt").
      toDF("content").withColumn("sentiLable", lit("厌恶"))//.limit(100)
    val label_3 = spark.read.format("text").load("file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/sentimentDic/weibo2/3_simplifyweibo.txt").
      toDF("content").withColumn("sentiLable", lit("低落"))//.limit(100)

    // load stop words
    val stopwordsFile: String = "file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/Stopwords.dic"
    val stopwords = sc.textFile(stopwordsFile).collect().toList


    val df = label_0.union(label_1).union(label_2).union(label_3)

    //将 df 保存到 sentiment_test 表中
    val url2 = "jdbc:mysql://localhost:3306/sunluMySQL?useUnicode=true&characterEncoding=UTF-8"
    //使用"?useUnicode=true&characterEncoding=UTF-8"以防止出现存入MySQL数据库中中文乱码情况
    //    val url = "jdbc:mysql://localhost:3306/sunluMySQL?useUnicode=true&characterEncoding=UTF-8&" +
    //      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    // 使用"useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"防止出现时间上的错误
    val prop2 = new Properties()
    prop2.setProperty("driver", "com.mysql.jdbc.Driver") //防止找不到driver
    prop2.setProperty("user", "root")
    prop2.setProperty("password", "root")

    //将结果保存到数据框中
    df.write.mode("overwrite").jdbc(url2, "sentiment_test", prop2) //overwrite ; append


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
      .setVectorSize(20)
      .setMinCount(1)
    val word2vecModel = word2Vec.fit(segDF)
    val word2vecDF = word2vecModel.transform(segDF)

    val Array(trainingDF, testDF) = word2vecDF.randomSplit(Array(0.8, 0.2), 1234L)

    val layers = Array(20, 5, 4,4)
    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)

    // train the model
    val mpcModel = trainer.fit(trainingDF)
    val predictions = mpcModel.transform(testDF)

    // 使用 LabelConverter 将预测结果的数值标签转换成原始的文本标签
    val labelConverter = new IndexToString().
      setInputCol("prediction").
      setOutputCol("predictedLabelCol").
      setLabels(labelIndexer.labels)

    val predictionsLabel = labelConverter.transform(predictions)
    predictionsLabel.printSchema()
    predictionsLabel.show()

    // 预测结果评价
    val evaluator = new MulticlassClassificationEvaluator().
      setLabelCol("label").
      setPredictionCol("prediction").
      setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("prdiction accuracy is: " + accuracy)


    sc.stop()
    spark.stop()
  }

}
