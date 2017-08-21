package com.evayInfo.Inglory.Project.DocsSimilarity


import com.evayInfo.Inglory.util.mysqlUtil
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{StringIndexer, Word2Vec, Word2VecModel}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.feature.{Word2VecModel => Word2VecModelRDD}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, MatrixEntry}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 17/8/14.
 * 使用word2vec方法生成features，使用改features计算文档之间的相似性
 *
 * ylzx_Example表中有3列，分别为rowkey, title, content
 * 使用表中的rowkey列生成每列的唯一标识
 *
 */
object word2VecSimiTest1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  case class docSimsSchema(doc1: String, doc2: String, sims: Double)


  def main(args: Array[String]) {
    SetLogger
    val conf = new SparkConf().setAppName(s"word2VecSimiTest1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

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
      .setMinCount(0)
    val model = word2Vec.fit(segDF)



    // save word2vec Model
    model.write.overwrite().save("result/word2vecModel")

    // reload word2vec Model
    val model2 = Word2VecModel.load("result/word2vecModel")
    //   val modelRDD = Word2VecModelRDD.load(sc, "result/word2vevModel")
    /*
    Exception in thread "main" org.json4s.package$MappingException: Did not find value which can be converted into java.lang.String
     */

    //    model.getVectors
    val word2VecDF = model.transform(segDF)

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

    val document = word2VecDF.select("id", "features").na.drop.rdd.map {
      case Row(id: Double, features: MLVector) => (id.toLong, Vectors.fromML(features))
    }.distinct
    //    document.take(5).foreach(println)
    /*
(597,[3.022767580905237E-4,0.033404000841481304,0.001301010748566867,-0.019667238256641448,-0.01756568674446477,..])
(275,[0.014117566789346902,0.0351425044028764,0.015634512888337358,0.01563165396909936,0.0067007435385936075,...])
(385,[0.014747553139758134,-0.01842303816034735,-0.01409709512917358,0.02076111010840902,-0.016556579455268235,......])
(787,[-0.013859073911526383,-0.003953272684485756,-0.029401518986006927,-0.03210853489712255,-0.0016232577657272216,...])
(860,[-0.009045543392242578,0.021420765830687465,0.0055320878159154045,-0.03331517241089247,0.010156096107004067,...])

     */

    val tfidf = document.map { case (i, v) => new IndexedRow(i, v) }
    val mat = new IndexedRowMatrix(tfidf)


    val transposed_matrix = mat.toCoordinateMatrix.transpose()

    /* The minimum cosine similarity threshold for each document pair */
    val threshhold = 0.5.toDouble
    val upper = 1.0

    val sim = transposed_matrix.toRowMatrix.columnSimilarities(threshhold)
    val exact = transposed_matrix.toRowMatrix.columnSimilarities()

    val sim_threshhold = sim.entries.filter { case MatrixEntry(i, j, u) => u >= threshhold && u <= upper }


    val docSimsRDD = sim_threshhold.map { x => {
      val doc1 = x.i.toString
      val doc2 = x.j.toString
      val sims = x.value.toDouble
      //保留sims中有效数字
      val sims2 = f"$sims%1.5f".toDouble
      docSimsSchema(doc1, doc2, sims2)
    }
    }

    val docSimsDF = spark.createDataset(docSimsRDD)

    //对dataframe进行分组排序，并取每组的前5个
    //计算两个向量的余弦相似度，值越大就表示越相似。
    val w = Window.partitionBy("doc1").orderBy(col("sims").desc)
    val ds5 = docSimsDF.withColumn("rn", row_number.over(w)).where(col("rn") <= 5)


    val wordsUrlLabDF = word2VecDF.withColumnRenamed("id", "doc2").withColumnRenamed("title", "title2").
      withColumnRenamed("rowkey", "rowkey2").
      select("doc2", "rowkey2", "title2")

    val wordsUrlLabDF2 = word2VecDF.withColumnRenamed("id", "doc1").select("doc1", "rowkey")

    val ds6 = ds5.join(wordsUrlLabDF, Seq("doc2"), "left")
    //doc1,doc2,sims,rn,url2id,title2,label2,time2,websitename2
    val ds7 = ds6.join(wordsUrlLabDF2, Seq("doc1"), "left").na.drop()

    //    ds7.show(5)
    /*
    +----+----+-------+---+--------------------+--------------------+--------------------+
    |doc1|doc2|   sims| rn|             rowkey2|              title2|              rowkey|
    +----+----+-------+---+--------------------+--------------------+--------------------+
    | 299| 708|0.94748|  1|cn.gov.sdfgw.www:...|国家发展改革委办公厅 农业部办公厅...|cn.gov.sdfgw.www:...|
    | 299| 530|0.93277|  3|cn.gov.sdfgw.www:...|关于印发《山东省高速公路网中长期规...|cn.gov.sdfgw.www:...|
    | 299| 572|0.94288|  2|cn.gov.miit.www:h...|两部门关于印发《新型墙材推广应用行...|cn.gov.sdfgw.www:...|
    | 299| 739|0.92696|  5|cn.gov.miit.www:h...|关于印发2014年互联网与工业融合...|cn.gov.sdfgw.www:...|
    | 299| 622|0.92706|  4|cn.gov.miit.www:h...|三部门关于印发《中国制造2025－...|cn.gov.sdfgw.www:...|
    +----+----+-------+---+--------------------+--------------------+--------------------+
     */


    /*
        val keyWords = Vector("科技", "人才")
        val synonyms2 = sc.parallelize(keyWords).map { x =>
          (x, try {
            modelRDD.findSynonyms(x, 100).map(_._1).mkString(";")
          } catch {
            case e: Exception => ""
          })
        }
        synonyms2.collect().foreach(println)

    */


    /*
java.lang.UnsupportedOperationException: Schema for type org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] is not supported
 */

    val df1 = sc.parallelize(Seq(("科技"), ("人才"))).toDF("keyWords")

    val word2VecFunc = udf((keyWords: String, num: Int) => {
      model2.findSynonyms(keyWords, num)
    })

    val df2 = df1.withColumn("synonymes", word2VecFunc(col("keyWords")))
    df1.show()
    df2.show()


    val keyWords1 = Vector("科技", "人才")
    model.findSynonyms("科技", 2).collect().foreach(println)
    model2.findSynonyms("科技", 2).collect().foreach(println)


    sc.stop()
    spark.stop()

  }

}
