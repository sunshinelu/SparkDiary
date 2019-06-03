package com.evayInfo.Inglory.Project.YLZX_ZTB

import java.text.SimpleDateFormat
import java.util.Properties

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{HashingTF, IDF, MinHashLSH}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.TimestampType

/*

计算文档相似性

参考链接：
1.Filtering a spark dataframe based on date
https://stackoverflow.com/questions/31994997/filtering-a-spark-dataframe-based-on-date
2.在 Spark DataFrame 中使用Time Window
https://blog.csdn.net/wangpei1949/article/details/83855223
 */
object DocSimi_Title {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {

    SetLogger

    //bulid environment
    val spark = SparkSession.builder.appName("DocSimi_Title").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //connect mysql database
    val url1 = "jdbc:mysql://10.20.5.49:3306/efp5-ztb"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "BigData@2018")
    //get data
    val ds1 = spark.read.jdbc(url1, "collect_ccgp", prop1)


    val col_names = Seq("id","title", "data", "website").map(col(_))
    val ds2 = ds1.select(col_names: _*)
    //    ds2.printSchema()
    //    ds2.show(5,truncate = false)

    def time_func(time: String): String = {
      val dateFormat_s1 = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
      val result = if (time.length() == 19) {
        val data_D_s1 = dateFormat_s1.parse(time)
        val data_S_s1 = dateFormat.format(data_D_s1)
        return data_S_s1
      } else if (time.length() == 10) {
        val data_D_s2 = dateFormat.parse(time)
        val data_S_s2 = dateFormat.format(data_D_s2)
        return data_S_s2
      } else {
        ""
      }
      return result
    }

    val time_udf = udf((time: String) => time_func(time))

    // 使用trim函数去除data列里面的空格
    val ds3 = ds2.withColumn("data", trim($"data")).
      withColumn("unified_time", time_udf($"data")).
      withColumn("unified_time", $"unified_time".cast(TimestampType)).
      withColumn("unified_time", date_format($"unified_time", "yyyy-MM-dd")).
      orderBy($"unified_time".desc).drop("data")
//    ds3.printSchema()
//    ds3.show(10, truncate = false)


//    ds3.select("unified_time").dropDuplicates().show()

//    ds3.filter(functions.year($"eventTime").between(2017,2018))

    val ds4 = ds3.filter($"unified_time" =!= "2019-05-10")
//    val ds4 = ds3.filter($"unified_time".lt(lit("2015-05-24")) && $"unified_time".gt(lit("2015-05-22")))
//    val ds4 = ds3.filter($"unified_time" <= "2019-05-25" && $"unified_time" >= "2019-05-23")

//    ds4.select("unified_time").dropDuplicates().show(truncate = false)

    /*
 using ansj seg words
  */
    def segWords(txt:String):String = {
      //    val segWords = ToAnalysis.parse(txt).toArray().map(_.toString.split("/")).map(_ (0)).toSeq.mkString(" ")
      //     segWords
      /*
      解决分词时文档中有 “/”的报错的问题
      参考资料：
      ansj分词教程
      https://blog.csdn.net/a360616218/article/details/75268959
       */
      val wordseg = ToAnalysis.parse(txt)
      var result = ""
      for (i <- 0 to wordseg.size() - 1){
        result = result + " " +  wordseg.get(i).getName()
      }
      result
    }
    val segWordsUDF = udf((txt: String) => segWords(txt))

    val ds5 = ds4.withColumn("seg_words",segWordsUDF($"title"))
    // 对word_seg中的数据以空格为分隔符转化成seq
    val ds6 = ds5.withColumn("seg_words_seq", split($"seg_words"," ")).drop("seg_words")

    /*
     calculate tf-idf value
      */
    val hashingTF = new HashingTF().
    setInputCol("seg_words_seq").
      setOutputCol("rawFeatures") .setNumFeatures(20000)

    val featurizedData = hashingTF.transform(ds6)
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
    //    mhTransformed.printSchema()

    val ds_shandong = mhTransformed.filter($"website" === "山东政府采购网")
    val ds_china = mhTransformed.filter($"website" === "中国政府采购网")

    val docsimi_mh = mhModel.approxSimilarityJoin(ds_shandong, ds_china, 1.0)

    //    docsimi_mh.printSchema()

    val colRenamed = Seq("shandong_Id", "china_id", "shandong_title", "china_title",
      "shandong_time", "china_time","shandong_website", "china_website","distCol")

    val mhSimiDF = docsimi_mh.select("datasetA.id", "datasetB.id",
      "datasetA.title", "datasetB.title",
      "datasetA.unified_time","datasetB.unified_time",
      "datasetA.website","datasetB.website",
      "distCol").
      toDF(colRenamed: _*).filter($"shandong_Id" =!= $"chine_id")

    val w = Window.partitionBy($"shandong_Id").orderBy($"distCol".desc)
    val ds_simi = mhSimiDF.withColumn("rn", row_number.over(w)).where($"rn" <= 5) //.drop("rn")
//    ds_simi.show(20,truncate = false)

    //将ds1保存到testTable2表中
    val url2 = "jdbc:mysql://localhost:3306/ylzx_ztb?useUnicode=true&characterEncoding=UTF-8"
    val prop2 = new Properties()
    prop1.setProperty("driver", "com.mysql.jdbc.Driver") //防止找不到driver
    prop2.setProperty("user", "root")
    prop2.setProperty("password", "root")

    //将结果保存到数据框中
//    ds_simi.write.mode("overwrite").jdbc(url2, "ztb_tltle_simi", prop2) //overwrite  append
    ds_simi.coalesce(5).write.mode("overwrite").jdbc(url1, "ztb_tltle_simi", prop1) //overwrite  append

    /*
    Caused by: java.io.FileNotFoundException: /tmp/blockmgr-7153be90-79d9-4f85-a33a-74e7f264fb13/12/temp_shuffle_dbae3fd5-ea85-4fd2-8fdc-5249e0e91d4a (打开的文件过多)

     */
    sc.stop()
    spark.stop()

  }

}
