package com.evayInfo.Inglory.Project.GW

import java.util.Properties

import breeze.linalg.DenseVector
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{MinHashLSH, CountVectorizerModel}
import org.apache.spark.ml.linalg.{Vectors => MLVectors}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._


/**
 * Created by sunlu on 18/8/7.
 */
object StringSimi {

  def main(args: Array[String]) {

    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"StringSimi").setMaster("local[*]").set("spark.executor.memory", "4g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val s = "亿云信息聚合政府、人才、企业等海量数据资源，依托“山东省电子政务大数据工程技术研究中心”实现政务大数据挖掘与分析，为公众和政府提供分析研判和辅助决策服务，"//共同推进山东省信息化建设和社会经济发展。"

    val url = "jdbc:mysql://localhost:3306/gwdl?useUnicode=true&characterEncoding=UTF-8&" +
      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val user = "root"
    val password = "root"
    val prop = new Properties()
    prop.setProperty("driver", "com.mysql.jdbc.Driver") //防止找不到driver
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    val mysql_Table = "features"

    /*
    val ds1 = spark.read.jdbc(url, mysql_Table, prop).withColumn("features",regexp_replace($"features","\\[","")).
      withColumn("features",regexp_replace($"features","\\]","")).withColumn("features", split($"features", ","))

    ds1.show()
    ds1.printSchema()

    val ds2 = ds1.rdd.map {
      case Row(id: String, features) => (id, DenseVector(features))
    }.toDF("id","features")

    ds2.show()
    ds2.printSchema()

*/

    /*
    https://stackoverflow.com/questions/30354483/convert-spark-row-to-typed-array-of-doubles
     */
    val df1 = spark.read.jdbc(url, mysql_Table, prop)
    val df2 = df1.rdd.map { case Row(id: String, features: String) =>
      val features_vector = features.replace("[", "").replace("]", "").split(",").
        toSeq.toArray.map(_.toDouble)
      val features_dense_vector = MLVectors.dense(features_vector).toSparse

      (id, features_dense_vector)
    }.toDF("id", "features")
    df2.printSchema()
    df2.show()

    val test_df1 = sc.parallelize(Seq(("test",s))).toDF("id","txt")
    /*
using ansj seg words
*/
    def segWords(txt:String):String = {
      val wordseg = ToAnalysis.parse(txt)
      var result = ""
      for (i <- 0 to wordseg.size() - 1){
        result = result + " " +  wordseg.get(i).getName()
      }
      result
    }
    val segWordsUDF = udf((txt: String) => segWords(txt))

    val test_df2 = test_df1.withColumn("seg_words",segWordsUDF($"txt"))
    // 对word_seg中的数据以空格为分隔符转化成seq
    val test_df3 = test_df2.withColumn("seg_words_seq", split($"seg_words"," ")).drop("txt").drop("seg_words")

    val cvModel_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/cvModel"
    val cvModel = CountVectorizerModel.load(cvModel_path)
    val test_df4 = cvModel.transform(test_df3).select("id","features")
    test_df4.printSchema()
    test_df4.show()


    /*
using Jaccard Distance calculate doc-doc similarity
  */
    val mh = new MinHashLSH().
      setNumHashTables(3).
      setInputCol("features").
      setOutputCol("mhValues")
    val mhModel = mh.fit(df2)

    // Feature Transformation
    val mhTransformed = mhModel.transform(df2)
    val docsimi_mh = mhModel.approxSimilarityJoin(test_df4, df2, 1.0)
    docsimi_mh.show()

    val colRenamed = Seq("doc1Id", "doc2Id","distCol")
    val mhSimiDF = docsimi_mh.select("datasetA.id", "datasetB.id","distCol").
      withColumn("distCol", bround($"distCol", 3)).
      toDF(colRenamed: _*).orderBy($"doc1Id".asc,$"distCol".asc)

    mhSimiDF.show()

    val result_list = mhSimiDF.rdd.map{case Row(doc1Id:String, doc2Id:String,distCol:Double) =>
      (doc1Id, doc2Id, distCol)}.collect().toList.asJava

    for(i <- result_list.asScala){
      println(i._1 + " vs " + i._2 + ": " + i._3)
    }


    sc.stop()
    spark.stop()

  }

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

}
