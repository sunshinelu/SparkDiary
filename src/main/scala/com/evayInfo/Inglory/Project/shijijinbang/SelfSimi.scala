package com.evayInfo.Inglory.Project.shijijinbang

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{MinHashLSH, IDF, HashingTF}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 18/7/23.
 */
object SelfSimi {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger
    //bulid environment
    val spark = SparkSession.builder.appName("SelfSimi").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val file_path = "file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/shijijinbang/self_simi_test1.txt"
    val colName = Seq("txt")
    val df1 = spark.read.textFile(file_path).toDF(colName: _*)
//    df1.show()

    // 新增一列递增列
    val w1 = Window.orderBy("txt")
    val df2 = df1.withColumn("id", row_number().over(w1))
    df2.show(false)

    def segWords(txt:String):String = {
      val segWords = ToAnalysis.parse(txt).toArray().map(_.toString.split("/")).map(_ (0)).toSeq.mkString(" ")
      segWords
    }
    val segWordsUDF = udf((txt: String) => segWords(txt))

    val df3 = df2.withColumn("seg_words",segWordsUDF($"txt"))
//    df3.show()

    // 对word_seg中的数据以空格为分隔符转化成seq
    val df4 = df3.withColumn("seg_words_seq", split($"seg_words"," ")).drop("txt").drop("seg_words")

    /*
   calculate tf-idf value
    */
    val hashingTF = new HashingTF().
      setInputCol("seg_words_seq").
      setOutputCol("rawFeatures") //.setNumFeatures(20000)

    val featurizedData = hashingTF.transform(df4)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().
      setInputCol("rawFeatures").
      setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val tfidfData = idfModel.transform(featurizedData)

    /*
using Jaccard Distance calculate doc-doc similarity
    */
    val mh = new MinHashLSH().
      setNumHashTables(3).
      setInputCol("features").
      setOutputCol("mhValues")
    val mhModel = mh.fit(tfidfData)

    // Feature Transformation
    val mhTransformed = mhModel.transform(tfidfData)
    val docsimi_mh = mhModel.approxSimilarityJoin(mhTransformed, mhTransformed, 1.0)

    val colRenamed = Seq("doc1Id", "doc1", "doc2Id", "doc2","distCol")

    val mhSimiDF = docsimi_mh.select("datasetA.id", "datasetA.seg_words_seq", "datasetB.id", "datasetB.seg_words_seq","distCol").
      toDF(colRenamed: _*).filter($"doc1Id" =!= $"doc2Id")

//    mhSimiDF.show(200)
    mhSimiDF.select("doc1Id","doc2Id","distCol").filter($"doc1Id" < $"doc2Id").orderBy($"doc1Id".asc,$"distCol".asc).show(200)


    print("==============================")

//    docsimi_mh.filter("datasetA.id < datasetB.id").show()

    print("++++++++++++++++++++++++++++++")

//    mhModel.approxSimilarityJoin(tfidfData, tfidfData, 0.6).filter("datasetA.id < datasetB.id").show()

    /*
 +-----------------------------------------------------------------------------------------------------------------------------------------------------+---+
|txt                                                                                                                                                  |id |
+-----------------------------------------------------------------------------------------------------------------------------------------------------+---+
|亿云信息专注于电子政务信息化服务，山东省内电子政务专家，为政府及企事业单位提供信息化规划、建设、运营等全方位的解决方案，业务范围覆盖政务平台搭建、大数据分析、智慧城市构建以及平台运营服务四大领域。聚合政府、人才、企业等海量数据资源，实现政务大数据挖掘与分析，为公众和政府提供分析研判和辅助决策服务。|1  |
|亿云信息专注于电子政务信息化服务，山东省内电子政务专家，为政府及企事业单位提供信息化规划、建设、运营等全方位的解决方案，业务范围覆盖政务平台搭建、大数据分析、智慧城市构建以及平台运营服务四大领域，形成了政务办公、申报评审、人才服务、资源共享、垂直搜索、运营服务等一系列成熟产品及解决方案。     |2  |
|亿云信息先后建设并运营省委组织部“人才山东”政务平台，服务泰山学者等高层次人才；建设运营中小企业公共服务平台，为全省中小企业和产业集群提供电商和供应链生态服务；建设运维省政务数据交换共享平台，为全省政务部门提供数据交换共享服务，形成智慧城市基础数据库。                       |3  |
|亿云信息聚合政府、人才、企业等海量数据资源，依托“山东省电子政务大数据工程技术研究中心”实现政务大数据挖掘与分析，为公众和政府提供分析研判和辅助决策服务，共同推进山东省信息化建设和社会经济发展。                                                    |4  |
|山东亿云信息技术有限公司成立于2011年，专注于电子政务信息化服务，为政府及企事业单位提供信息化规划、建设、运营等全方位的解决方案，业务范围覆盖政务平台搭建、大数据分析、智慧城市构建以及平台运营服务四大领域。                                             |5  |
|山东亿云信息技术有限公司成立于2011年，专注于电子政务信息化服务，为政府及企事业单位提供信息化规划、建设、运营等全方位的解决方案，业务范围覆盖政务平台搭建、大数据分析、智慧城市构建以及平台运营服务四大领域。                                             |6  |
+-----------------------------------------------------------------------------------------------------------------------------------------------------+---+

+------+------+-------------------+
|doc1Id|doc2Id|            distCol|
+------+------+-------------------+
|     1|     2| 0.4385964912280702|
|     1|     5|0.47058823529411764|
|     1|     6|0.47058823529411764|
|     1|     4|  0.603448275862069|
|     2|     6|             0.4375|
|     2|     5|             0.4375|
|     2|     4| 0.8695652173913043|
|     4|     6| 0.9016393442622951|
|     4|     5| 0.9016393442622951|
|     5|     6|                0.0|
+------+------+-------------------+

     */

//    print(spark.version)
    sc.stop()
    spark.stop()


  }

}
