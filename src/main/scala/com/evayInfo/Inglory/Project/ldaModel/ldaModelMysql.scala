package com.evayInfo.Inglory.Project.ldaModel

import com.evayInfo.Inglory.util.mysqlUtil
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.{StringIndexer, Word2Vec}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/8/18.
 * 读取本地mysql数据构建LDA主题模型
 *
 */
object ldaModelMysql {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger
    val conf = new SparkConf().setAppName(s"ldaModelMysql").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    val url = "jdbc:mysql://localhost:3306/sunluMySQL"
    //    val url1 = "jdbc:mysql://localhost:3306/sunluMySQL?useUnicode=true&characterEncoding=UTF-8&" +
    //      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val user = "root"
    val password = "root"

    val ylzxTable = "ylzx_Example"
    val ylzxDF = mysqlUtil.getMysqlData(spark, url, user, password, ylzxTable)

    val indexer = new StringIndexer()
      .setInputCol("rowkey")
      .setOutputCol("id")

    val indexed = indexer.fit(ylzxDF).transform(ylzxDF)
    //    indexed.show()


    //load stopwords file
    val stopwordsFile = "data/Stopwords.dic"
    val stopwords = sc.textFile(stopwordsFile).collect().toList

    //定义UDF
    //分词、停用词过滤
    val segWorsd = udf((content: String) => {
      ToAnalysis.parse(content).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 1 & !stopwords.contains(word)).toSeq
    })

    val segDF = indexed.withColumn("segWords", segWorsd(column("content")))

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("segWords")
      .setOutputCol("features")
      .setVectorSize(10) // 1000
      .setMinCount(2)

    val word2VecModel = word2Vec.fit(segDF)
    val word2VecDF = word2VecModel.transform(segDF)

    //    word2VecDF.printSchema()
    /*
    root
     |-- rowkey: string (nullable = true)
     |-- title: string (nullable = true)
     |-- content: string (nullable = true)
     |-- id: double (nullable = true)
     |-- segWords: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- features: vector (nullable = true)
    */

    /*
    val dataset = spark.read.format("libsvm").load("/Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/data/sample_lda_data.txt")

    dataset.printSchema()
    dataset.show(5)
*/
    /*

    import org.apache.spark.ml.clustering.LDA

    // Loads data.
    val dataset = spark.read.format("libsvm").load("/Users/sunlu/Software/spark-2.0.2-bin-hadoop2.6/data/mllib/sample_lda_libsvm_data.txt")

*/


    // Trains a LDA model.
    val lda = new LDA().setK(10).setMaxIter(10)
    val ldaModel = lda.fit(word2VecDF.select("id", "features"))

    val ll = ldaModel.logLikelihood(word2VecDF)
    val lp = ldaModel.logPerplexity(word2VecDF)
    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println(s"The upper bound on perplexity: $lp")

    // Describe topics.
    val topics = ldaModel.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show(false)

    // Shows the result.
    val transformed = ldaModel.transform(word2VecDF)
    transformed.show(5)
    transformed.printSchema()


    sc.stop()
    spark.stop()
  }

}
