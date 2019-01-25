package com.evayInfo.Inglory.Project.RenCai

import java.util.Properties


import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, MinHashLSH}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, MatrixEntry}
import org.apache.spark.sql.functions.{row_number, split, udf,bround}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.expressions.Window

object SimiCompanyName {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  /*
  using ansj seg words
   */
  def segWords(txt:String):String = {
    val wordseg = ToAnalysis.parse(txt)
    var result = ""
    for (i <- 0 to wordseg.size() - 1){
      result = result + " " +  wordseg.get(i).getName()
    }
    result.trim()
  }

  case class companySchema(id_1: String, id_2: String, sims: Double)

  def main(args: Array[String]): Unit = {
    //不打印日志信息
    SetLogger

    val conf = new SparkConf().setAppName(s"TongShiRelation").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //connect mysql database
    val url1 = "jdbc:mysql://localhost:3306/talent"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")

    //get data
    val ds1 = spark.read.jdbc(url1, "work_info", prop1).
      select("company").distinct().na.drop().filter($"company".contains("IBM")).
      limit(1000)

    // 新增一列递增列
    val w1 = Window.orderBy("company")
    val ds2 = ds1.withColumn("id", row_number().over(w1))

    val company_info_1 = ds2.select("id","company").toDF("id_1","company_1")
    val company_info_2 = ds2.select("id","company").toDF("id_2","company_2")


    val segWordsUDF = udf((txt: String) => segWords(txt))

    val ds3 = ds2.withColumn("seg_words",segWordsUDF($"company"))
    // 对word_seg中的数据以空格为分隔符转化成seq
    val ds4 = ds3.withColumn("seg_words_seq", split($"seg_words"," ")).
      drop("seg_words")

    // fit a CountVectorizerModel from the corpus
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("seg_words_seq")
      .setOutputCol("features")
//      .setVocabSize(3)
      .setMinDF(1)
      .fit(ds4)
    val ds5 = cvModel.transform(ds4)//.select("id","company","features")
    ds5.show(truncate = false)

    /*
        using Cosin calculate doc doc similarity
         */
    val features_rdd = ds5.select("id","features").rdd.map {
      case Row(id: Int, features: MLVector) => IndexedRow(id, Vectors.fromML(features))
    }
    /* The minimum cosine similarity threshold for each document pair */
    val threshhold = 0.5
    val upper = 1.0

    val features_index_rdd = new IndexedRowMatrix(features_rdd)
    val mat = features_index_rdd.toCoordinateMatrix.transpose()
    val sim = mat.toRowMatrix.columnSimilarities(threshhold)
    val sim2 = sim.entries.map { case MatrixEntry(i, j, u) => MatrixEntry(i, j, u) }

    val companySimsRDD = sim2.map { x => {
      val id_1 = x.i.toString
      val id_2 = x.j.toString
      val sims = x.value
      companySchema(id_1, id_2, sims)
    }
    }
    val ds6 = spark.createDataset(companySimsRDD)

    val ds7 = ds6.join(company_info_1,Seq("id_1"),"left").
      join(company_info_2,Seq("id_2"),"left").
//      filter($"company_1" =!= $"company_2").
      withColumn("sims", bround($"sims",3)).
      filter($"sims" >= 0.0)
    ds7.show(truncate = false)

    /*
    val ds2 = ds1.select("company").distinct().na.drop().limit(100)

//    println("ds2 is: " + ds2.count())//ds2 is: 42093

    val list3 = ds2.rdd.map{case Row(x:String) => (x)}.collect().toList

    val list4 = list3.sorted.combinations(2).toList.map(x => (x(0),x(1)))

    val ds5 = sc.parallelize(list4).toDF()//"company"
//    ds5.show(truncate = false)

//    println(ds2.count())


    val s = "加利福尼亚大学(河滨分校) University of California (Riverside)"
    var s2 = ""
    for(i <- s){
      s2 = s2 + i + " "
    }
    println(s2.trim)
*/

    sc.stop()
    spark.stop()
  }
}
