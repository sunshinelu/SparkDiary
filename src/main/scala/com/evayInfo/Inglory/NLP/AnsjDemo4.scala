package com.evayInfo.Inglory.NLP

import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.util.MyStaticValue
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRowMatrix, IndexedRow}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{Vector => MLVector}

/**
 * Created by sunlu on 17/10/25.
 * 分词、词性过滤、计算权重
 */
object AnsjDemo4 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"AnsjDemo4").setMaster("local[*]").
      set("spark.executor.memory", "2g")//.set("spark.Kryoserializer.buffer.max","2048mb")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df = sc.parallelize(Seq(
      (1L, "MMLSpark", "MMLSpark：微软开源的用于Spark的深度学习库"),
      (2L, "数据可视化", "大数据时代当城市数据和社会关系被可视化，每个人都可能是福尔摩斯"),
      (3L,"十九大","【权威发布】中国共产党第十九届中央委员会候补委员名单"))).
      toDF("id", "title", "content")
    df.show()

    // load stop words
    val stopwordsFile = "file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/Stopwords.dic"
    val stopwords = sc.textFile(stopwordsFile).collect().toList



    // load user defined dic

    val userDefineFile= "file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/library/userDefine.dic"
    val userDefineList = sc.textFile(userDefineFile)//.collect().toList
    userDefineList.foreach(x => {
      UserDefineLibrary.insertWord(x, "userDefine", 1000)
    })

//    MyStaticValue.userLibrary = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/library/userDefine.dic"


    //定义UDF
    //分词、词性过滤

    def getKeyWordsFunc(title: String, content: String): String = {
      //每篇文章进行分词
      val segContent = title + " " + content
//      MyStaticValue.userLibrary = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/library/userDefine.dic"

      /*
      userDefineList.foreach(x => {

        UserDefineLibrary.insertWord(x, "userDefine", 1000)
      })
*/
      val segWords = ToAnalysis.parse(segContent).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).filter(x => x(1).contains("n") || x(1).contains("userDefine") || x(1).contains("m")).
        map(_ (0)).toList.
        filter(word => word.length >= 2 & !stopwords.contains(word)).mkString(" ")

      val segWords2 = ToAnalysis.parse(segContent).toString
      val result = segWords match {
        case r if (r.length >= 2) => r
        case _ => "NULL" // Seq("null")
      }
      result
    }
    val KeyWordsUDF = udf((title: String, content: String) => getKeyWordsFunc(title, content))

    // get keywords based on title and content
    val df1 = df.withColumn("getKW", KeyWordsUDF($"title", $"content")).
      drop("content").drop("title").
      filter($"getKW" =!= "NULL")
    val df2 = df1.withColumn("words", explode(split($"getKW", " ")))
    df2.show(100, false)

//    val wordsTotal = df2.groupBy("words").agg(sum("tag"))// 每个词的总词频
    val wordsInDocTotal = df2.withColumn("tag1", lit(1.0)).groupBy("id","words").
  agg(sum("tag1"))//.withColumnRenamed("w","sum(tag1)")//每篇文章、每个词的词频
    val docsTotal = df2.withColumn("tag2", lit(1.0)).groupBy("id").agg(sum("tag2"))//.withColumnRenamed("sum_w","sum(tag2)")//每篇文章词的总数
    val docTotal = df2.select("id").distinct().count// 文章数据量
    val wordToDoc = wordsInDocTotal.
        withColumn("tag3", lit(1.0)).groupBy("words").agg(sum("tag3"))//.withColumnRenamed("doc","sum(tag3)")// 词对应的文章数据

    val df3 = df2.withColumn("total", lit(docTotal))
    val df4 = wordsInDocTotal.join(df3, Seq("id","words"),"left").
      join(docsTotal, Seq("id"), "left").
      join(wordToDoc, Seq("words"), "left").na.drop()
//    val df4 = df3.withColumn("if", $"w" / $"sum_w").
//      withColumn("idf", log($"total" / $"doc")).withColumn("tf-idf", $"tf" * "idf")
    val df5 = df4.withColumn("tf", $"sum(tag1)" / $"sum(tag2)").
      withColumn("idf", log($"total" / $"sum(tag3)")).withColumn("tf_idf", $"tf" * $"idf")
    df5.select("id", "words","tf", "idf","tf_idf")show(false)

    /*
    词频：tf = wordsInDocTotal / docsTotal
    逆向文件频率: idf = log(docTotal / wordToDoc)
    tf-idf = tf * idf
     */


    val tokenizer = new Tokenizer().setInputCol("getKW").setOutputCol("words")
    val wordsData = tokenizer.transform(df1)

    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setVocabSize(2000)
      .setMinDF(1)
      .fit(wordsData)
    val cvData = cvModel.transform(wordsData)

    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(2000)
    val featurizedData = hashingTF.transform(wordsData)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(cvData)
    val rescaledData = idfModel.transform(cvData)

    hashingTF.numFeatures
    featurizedData.columns.foreach(println)



   val wordsList =  cvModel.vocabulary
    wordsList.take(5).foreach(println)
    println(cvModel.vocabSize.toString())

    rescaledData.select("id","features").show(false)

    val document = rescaledData.select("id", "features").rdd.map {
      case Row(id: Long, features: MLVector) => IndexedRow(id, Vectors.fromML(features))
    }
    val indexed_matrix = new IndexedRowMatrix(document)
    indexed_matrix.rows.map { x =>
      val v = x.vector
      (x.index, v(0), v(1), v(2))
    }.collect().foreach(println)




    sc.stop()
    spark.stop()
  }

}
