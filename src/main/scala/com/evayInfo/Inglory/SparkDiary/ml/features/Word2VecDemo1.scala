package com.evayInfo.Inglory.SparkDiary.ml.features

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

//import org.apache.spark.ml.linalg.Vector

import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.Row

/**
 * Created by sunlu on 17/8/14.
 *
 * spark官网Word2Vec Demo
 *
 */
object Word2VecDemo1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger
    val conf = new SparkConf().setAppName(s"Word2VecDeom1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext


    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")
    documentDF.show()
    /*
+--------------------+
|                text|
+--------------------+
|[Hi, I, heard, ab...|
|[I, wish, Java, c...|
|[Logistic, regres...|
+--------------------+

     */


    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)

    val word2VecResult = model.transform(documentDF)

    word2VecResult.printSchema()
    /*
    root
 |-- text: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- result: vector (nullable = true)

     */

    word2VecResult.show()
    /*
+--------------------+--------------------+
|                text|              result|
+--------------------+--------------------+
|[Hi, I, heard, ab...|[0.01849065460264...|
|[I, wish, Java, c...|[0.05958533100783...|
|[Logistic, regres...|[-0.0110558800399...|
+--------------------+--------------------+

     */

    word2VecResult.collect().foreach { case Row(text: Seq[_], features: MLVector) =>
      println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n")
    }
    /*
Text: [Hi, I, heard, about, Spark] =>
Vector: [0.018490654602646827,-0.016248732805252075,0.04528368394821883]

Text: [I, wish, Java, could, use, case, classes] =>
Vector: [0.05958533100783825,0.023424440695505054,-0.027310076036623544]

Text: [Logistic, regression, models, are, neat] =>
Vector: [-0.011055880039930344,0.020988055132329465,0.042608972638845444]

     */


    val document = word2VecResult.select("result").na.drop.rdd.map {
      case Row(features: MLVector) => (Vectors.fromML(features))
    }.distinct
    document.collect().foreach(println)
    /*
[0.05958533100783825,0.023424440695505054,-0.027310076036623544]
[-0.011055880039930344,0.020988055132329465,0.042608972638845444]
[0.018490654602646827,-0.016248732805252075,0.04528368394821883]
     */


    val testDF1 = spark.createDataFrame(Seq(
      "This is test Demo".split(" "),
      "The test Demo about word2vec".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")
    testDF1.show()


    val test1Result = model.transform(testDF1)
    test1Result.show()

    test1Result.collect().foreach { case Row(text: Seq[_], features: MLVector) =>
      println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n")
    }
    /*

    Text: [This, is, test, Demo] =>
    Vector: [0.0,0.0,0.0]

    Text: [The, test, Demo, about, word2vec] =>
    Vector: [-0.030008900165557864,-0.009876188635826112,0.0066151686012744905]

    Text: [Logistic, regression, models, are, neat] =>
    Vector: [-0.011055880039930344,0.020988055132329465,0.042608972638845444]

     */

    val testDF2 = spark.createDataFrame(Seq(
      "Hi I Logistic about Spark".split(" "),
      "I wish models could use  classes".split(" "),
      "regression are neat case".split(" ")
    ).map(Tuple1.apply)).toDF("text")
    testDF2.show()


    val test2Result = model.transform(testDF2)
    test2Result.show()

    test2Result.collect().foreach { case Row(text: Seq[_], features: MLVector) =>
      println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n")
    }

    /*
Text: [Hi, I, Logistic, about, Spark] =>
Vector: [-0.03681530021131039,0.025903867185115816,0.03252785932272673]

Text: [I, wish, models, could, use, , classes] =>
Vector: [0.040678041587982855,0.005992020713165402,0.0011639637606484549]

Text: [regression, are, neat, case] =>
Vector: [0.06121007725596428,0.025119595229625702,0.07006354443728924]

     */

    sc.stop()
    spark.stop()


  }

}
