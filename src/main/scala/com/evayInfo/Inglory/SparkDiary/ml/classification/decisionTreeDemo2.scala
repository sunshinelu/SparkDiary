package com.evayInfo.Inglory.SparkDiary.ml.classification

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/12/8.
 * 读取data/sentimentDic/weibo中的数据构建四分类模型
 *
 */
object decisionTreeDemo2 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"decisionTreeDemo2").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val toUDF8 = udf((arg:String) =>
//      new String( new String(arg.getBytes(), "GB2312").getBytes(), "GBK")
//    new java.lang.String(arg.getBytes(), "GB2312")
//      new String(new String(arg.getBytes(), "GB2312").getBytes(), "UTF-8")
      new String(arg.getBytes,0, arg.length, "GB2312")

//    new String(arg.getBytes, "utf8")
//new java.lang.String(new java.lang.String(arg.getBytes(), "GBK").getBytes("GBK"),"UTF-8")
    )

    val getCode = udf((arg:String) => arg.getBytes().length)

    // load sentiment datasets
    val label_0 = spark.read.format("text").load("file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/sentimentDic/weibo/0_simplifyweibo.txt").
      toDF("content").withColumn("Lable", lit("喜悦")).
      withColumn("content2", toUDF8($"content")).
      withColumn("code" , getCode($"content"))
    val label_1 = spark.read.format("text").load("file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/sentimentDic/weibo/1_simplifyweibo.txt").
      toDF("content").withColumn("Lable", lit("愤怒")).
      withColumn("content2", toUDF8($"content")).
      withColumn("code" , getCode($"content"))
    val label_2 = spark.read.format("text").load("file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/sentimentDic/weibo/2_simplifyweibo.txt").
      toDF("content").withColumn("Lable", lit("厌恶")).
      withColumn("content2", toUDF8($"content")).
      withColumn("code" , getCode($"content"))
    val label_3 = spark.read.format("text").load("file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/sentimentDic/weibo/3_simplifyweibo.txt").
      toDF("content").withColumn("Lable", lit("低落")).
      withColumn("content2", toUDF8($"content")).
      withColumn("code" , getCode($"content"))

    // load stop words
    val stopwordsFile: String = "file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/Stopwords.dic"
    val stopwords = sc.textFile(stopwordsFile).collect().toList


    val df = label_0.union(label_1).union(label_2).union(label_3)

    // split training set and tset set
    val Array(trainDF, testDF) = df.randomSplit(Array(0.005,0.999))

   println("training set is: " + trainDF.count())
    trainDF.show()
    /*
    // seg words
        val SegwordsUDF = udf((content:String) => content.split(" ").map(_.split("/")(0)).filter(x => ! stopwords.contains(x)).toSeq)
    val segDF = trainDF.withColumn("seg", SegwordsUDF($"content"))

    /*
 calculate tf-idf value
  */
    val hashingTF = new HashingTF().
      setInputCol("seg").
      setOutputCol("rawFeatures") //.setNumFeatures(20000)

    val featurizedData = hashingTF.transform(segDF)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().
      setInputCol("rawFeatures").
      setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val tfidfData = idfModel.transform(featurizedData)



    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("Lable")
      .setOutputCol("indexedLabel")
      .fit(tfidfData)
//    val labelDF = labelIndexer.transform(tfidfData)


/*
    //分词、停用词过滤
    def segWordsFunc( content: String): Seq[String] = {
      val seg = content.split(" ").map(_.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 1 & !stopwords.contains(word)).toSeq
      val result = seg match {
        case r if (r.length >= 1) => r
        case _ => Seq("") // Seq("null")
      }
      result
    }

    val SegwordsUDF = udf((content:String) => segWordsFunc(content))

    // seg words
//    val SegwordsUDF = udf((content:String) => content.split(" ").map(_.split("/")(0)).filter(x => ! stopwords.contains(x)).toSeq)

    val trainDF2 = trainDF.withColumn("seg", SegwordsUDF($"content"))
//    trainDF2.printSchema()
*/
    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer()
  .setInputCol("features")
  .setOutputCol("indexedFeatures")
  .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
  .fit(tfidfData)

    // Train a DecisionTree model.
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, dt,labelConverter))

    // Train model. This also runs the indexers.
    val model = pipeline.fit(tfidfData)

    // Make predictions.
    val predictions = model.transform(tfidfData)
    predictions.printSchema()
*/
    sc.stop()
    spark.stop()

  }
}
