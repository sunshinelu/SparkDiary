package com.evayInfo.Inglory.Project.ldaModel.Test

import java.util.Properties

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 17/4/24.
 * 读取本地mysql数据库中的ylzx_Example数据，对表中的数据构建聚类模型
 * 运行成功！
 */
object LDATest2 {

  case class ArticalLable(id: Long, segWords: Seq[String], rowkey: String)

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }

  def main(args: Array[String]) {
    //不打印日志信息
    SetLogger

    val conf = new SparkConf().setAppName(s"LDATest2").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //load stopwords file
    val stopwordsFile = "data/Stopwords.dic"
    val stopwords = sc.textFile(stopwordsFile).collect().toList

    //connect mysql database
    val url1 = "jdbc:mysql://localhost:3306/sunluMySQL"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")
    //get data
    val ds1 = spark.read.jdbc(url1, "ylzx_Example", prop1)
    val rdd1 = ds1.rdd.map(r => (r(0), r(1), r(2))).map { x =>
      val rowkey = x._1.toString
      val title = x._2.toString
      val content = x._3.toString
      val segWords = ToAnalysis.parse(content).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 2 & !stopwords.contains(word)).toSeq
      (rowkey, segWords)
    }.zipWithIndex().map { x =>
      val rowkey = x._1._1.toString
      val segWords = x._1._2
      val id = x._2
      ArticalLable(id, segWords, rowkey)
    }

    val ds2 = spark.createDataset(rdd1)
    //Converting the Tokens into the CountVector
    val vocabSize: Int = 2900000
    // val countVectorizer = new CountVectorizer().setVocabSize(vocabSize).setInputCol("segWords").setOutputCol("features")

    // val ds3 = countVectorizer.fit(ds2)


    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("segWords")
      .setOutputCol("features")
      .setVocabSize(vocabSize)
      .setMinDF(2)
      .fit(ds2)

    val ds3 = cvModel.transform(ds2)

    val corpus = ds3.select("features").rdd.map {
      case Row(features: MLVector) => Vectors.fromML(features)
    }.zipWithIndex().map(_.swap)

    val document = ds3.select("id", "features").rdd.map {
      case Row(id: Long, features: MLVector) => (id, Vectors.fromML(features))
    }

    val document2 = ds3.select("id", "features").rdd.map {
      case Row(id: Long, features: MLVector) => (id, Vectors.fromML(features))
    }

    val vocabArray = cvModel.vocabulary // vocabulary
    val actualCorpusSize = ds3.count()


    println("vocabArray is: " + vocabArray.mkString(";"))

    // ds3.show(10)

    // Run LDA.
    val lda = new LDA()

    val k: Int = 3
    val maxIterations: Int = 10
    val docConcentration: Double = -1
    val topicConcentration: Double = -1
    val checkpointDir: Option[String] = None
    val checkpointInterval: Int = 10
    val algorithm: String = "em"

    val optimizer = algorithm.toLowerCase match {
      case "em" => new EMLDAOptimizer
      // add (1.0 / actualCorpusSize) to MiniBatchFraction be more robust on tiny datasets.
      case "online" => new OnlineLDAOptimizer().setMiniBatchFraction(0.05 + 1.0 / actualCorpusSize)
      case _ => throw new IllegalArgumentException(
        s"Only em, online are supported but got ${algorithm}.")
    }

    lda.setOptimizer(optimizer)
      .setK(k)
      .setMaxIterations(maxIterations)
      .setDocConcentration(docConcentration)
      .setTopicConcentration(topicConcentration)
      .setCheckpointInterval(checkpointInterval)

    val ldaModel = lda.run(document)


    if (ldaModel.isInstanceOf[DistributedLDAModel]) {
      val distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]
      val avgLogLikelihood = distLDAModel.logLikelihood / actualCorpusSize.toDouble
      println(s"\t Training data average log likelihood: $avgLogLikelihood")
      println()
    }
    // Print the topics, showing the top-weighted terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 20)
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
    }

    val topics2 = topics.zipWithIndex.map { x =>
      val vec = x._1.toIterable
      val id = x._2
      (id, vec)
    }.flatMap { x =>
      val y = x._2
      for (w <- y) yield (x._1, w._1, w._2)
    }
    topics2.take(50).foreach(println)


    println(s"${k} topics:")
    topics.zipWithIndex.foreach { case (topic, i) =>
      println(s"TOPIC $i")
      topic.foreach { case (term, weight) =>
        println(s"$term\t$weight")
      }
      println()
    }
    /*
    val distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]
    val docVectors = distLDAModel.topicDistributions.map(_._2)

    val keansMaxIterations = 100.toInt
    val clusters = KMeans.train(docVectors,k,keansMaxIterations)

    val WSSSE = clusters.computeCost(docVectors)
*/
    spark.stop()

  }
}
