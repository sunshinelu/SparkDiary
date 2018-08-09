package com.evayInfo.Inglory.Project.GW

import java.util.{Properties, UUID}

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{SaveMode, Row, SparkSession}
import org.apache.spark.sql.functions._


/**
 * Created by sunlu on 18/8/9.
 */
object ExtractFeatures {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }




  def main(args: Array[String]) {

    SetLogger
    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"ExtractFeatures").setMaster("local[*]").set("spark.executor.memory", "4g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val url = "jdbc:mysql://localhost:3306/gwdl?useUnicode=true&characterEncoding=UTF-8&" +
      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val user = "root"
    val password = "root"
    val prop = new Properties()
    prop.setProperty("driver", "com.mysql.jdbc.Driver") //防止找不到driver
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    val mysql_Table = "features"

    val cvModel_path = "file:///Users/sunlu/Desktop/cvModel"
    val cvModel = CountVectorizerModel.load(cvModel_path)

    val str_1 = "亿云信息聚合政府、人才、企业等海量数据资源，依托“山东省电子政务大数据工程技术研究中心”实现政务大数据挖掘与分析，"//为公众和政府提供分析研判和辅助决策服务，共同推进山东省信息化建设和社会经济发展。"
    val id_1 = UUID.randomUUID().toString().toLowerCase()
    val test_df1 = sc.parallelize(Seq((id_1,str_1))).toDF("id","txt")
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
    val test_df4 = cvModel.transform(test_df3).select("id","features")

    test_df4.rdd.map {
      case Row(id: String, features: MLVector) => (id, Vectors.fromML(features).toDense.toString())
    }.toDF("id","features").coalesce(1).
      write.mode(SaveMode.Append).jdbc(url, mysql_Table, prop)


    sc.stop()
    spark.stop()
  }

}
