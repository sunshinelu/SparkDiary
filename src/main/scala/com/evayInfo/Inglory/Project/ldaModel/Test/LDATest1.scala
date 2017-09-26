package com.evayInfo.Inglory.Project.ldaModel.Test

import com.evayInfo.Inglory.Project.ldaModel.LdaUtil
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/9/26.
 * 使用测试文本构建LDA主题模型
 */
object LDATest1 {

  def main(args: Array[String]) {
    LdaUtil.SetLogger

    val sparkConf = new SparkConf().setAppName(s"LDATest1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val ldaDocument = sc.textFile("file:///Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/data/ldaDocuments.txt").
      toDF("content")
    val indexer = new StringIndexer()
      .setInputCol("content")
      .setOutputCol("id")

    val indexed = indexer.fit(ldaDocument).transform(ldaDocument)

    val stopwordsFile = "file:///Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/data/Stopwords.dic"
    val stopwords = sc.textFile(stopwordsFile).collect().toList
    //    val stopwords = sc.broadcast(sc.textFile(stopwordsFile).collect().toList)

    //定义UDF
    //分词、停用词过滤
    def segWordsFunc(content: String): Seq[String] = {
      val seg = ToAnalysis.parse(content).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 1 & !stopwords.contains(word)).toSeq
      val result = seg match {
        case r if (r.length >= 1) => r
        case _ => Seq("") // Seq("null")
      }
      result
    }

    val segWordsUDF = udf((content: String) => segWordsFunc(content))
    val segDF = indexed.withColumn("segWords", segWordsUDF(column("content"))) //.filter(!$"segWords".contains("null"))

    val vocabSize: Int = 2900000
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("segWords")
      .setOutputCol("features")
      .setVocabSize(vocabSize)
      .setMinDF(1)
      .fit(segDF)
    val cvDF = cvModel.transform(segDF)
    val vocabArray = cvModel.vocabulary // vocabulary
    println("vocabArray is: " + vocabArray.mkString(";"))

    val idf = new IDF().
      setInputCol("features").
      setOutputCol("ifidfFeatures")
    val idfModel = idf.fit(cvDF)
    val tfidfTF = idfModel.transform(cvDF)

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("segWords")
      .setOutputCol("features")
      .setVectorSize(100) // 1000
      .setMinCount(1)
    val word2VecModel = word2Vec.fit(segDF)
    val word2VecDF = word2VecModel.transform(segDF)
    val wordsDF = word2VecModel.getVectors
    wordsDF.show(false)

    // Trains a LDA model.
    val lda = new LDA().setK(3).setMaxIter(10)
    val lda_d = new LDA().setOptimizer("em").setK(4).setMaxIter(10)
    val ldaModel = lda_d.fit(tfidfTF)

    // Describe topics.
    val topics = ldaModel.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show(false)

    println(ldaModel.isDistributed)

    //    val distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]
    //    val topics_dist = distLDAModel.describeTopics(3)
    //    topics_dist.show(false)

    val ldaDF = ldaModel.transform(tfidfTF)
    ldaDF.printSchema()
    ldaDF.show(false)

    val topic_d = ldaModel.topicsMatrix
    println(topic_d.numCols)
    println(topic_d.numRows)

    sc.stop()
    spark.stop()
  }

}
