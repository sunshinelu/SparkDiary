package com.evayInfo.Inglory.Project.shijijinbang

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, MatrixEntry}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 18/7/23.
 */
object SelfSimiCosin {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  case class docSimsSchema(doc1: String, doc2: String, sims: Double)

  def main(args: Array[String]) {

    SetLogger
    //bulid environment
    val spark = SparkSession.builder.appName("SelfSimiCosin").master("local[*]").getOrCreate()
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

//    tfidfData.printSchema()

    /*
    using Cosin calculate doc doc similarity
     */
    val features_rdd = tfidfData.select("id","features").rdd.map {
      case Row(id: Int, features: MLVector) => IndexedRow(id, Vectors.fromML(features))
    }
    /* The minimum cosine similarity threshold for each document pair */
    val threshhold = 0.5.toDouble
    val upper = 1.0

    val features_index_rdd = new IndexedRowMatrix(features_rdd)
    val mat = features_index_rdd.toCoordinateMatrix.transpose()
    val sim = mat.toRowMatrix.columnSimilarities(threshhold)
    val sim2 = sim.entries.map { case MatrixEntry(i, j, u) => MatrixEntry(i, j, u) }

    val docSimsRDD = sim2.map { x => {
      val doc1 = x.i.toString
      val doc2 = x.j.toString
      val sims = x.value
      docSimsSchema(doc1, doc2, sims)
    }
    }
    val docSimsDS = spark.createDataset(docSimsRDD)

    docSimsDS.orderBy($"doc1".asc,$"sims".desc).show(200)

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

+----+----+--------------------+
|doc1|doc2|                sims|
+----+----+--------------------+
|   1|   4|  0.6014768314438395|
|   1|   6| 0.29459935507215995|
|   1|   5| 0.29459935507215995|
|   1|   2| 0.29322281518356147|
|   1|   3| 0.02339472825664732|
|   2|   6|  0.2997163702383314|
|   2|   5|  0.2997163702383314|
|   2|   4| 0.05338061629220941|
|   2|   3|  0.0101918584509778|
|   3|   4| 0.09536895385157487|
|   3|   6|0.023114241426396363|
|   3|   5|0.023114241426396363|
|   4|   5| 0.02473053078641149|
|   4|   6| 0.02473053078641149|
|   5|   6|  1.0000000000000002|
+----+----+--------------------+
     */
    sc.stop()
    spark.stop()
  }

}
