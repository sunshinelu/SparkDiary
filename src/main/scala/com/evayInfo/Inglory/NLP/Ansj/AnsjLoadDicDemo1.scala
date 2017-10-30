package com.evayInfo.Inglory.NLP.Ansj

import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/10/30.
 *
 * 测试：
 * 将“大数据”和“十九大”添加到library/default.dic文件中，并添加library.properties文件，在文件中指定词典的路径
 *
 * 测试代码
spark-submit \
--class com.evayInfo.Inglory.NLP.Ansj.AnsjLoadDicDemo1 \
--master yarn \
--deploy-mode client \
--num-executors 2 \
--executor-cores 1 \
--executor-memory 1g \
/root/lulu/Progect/Test/SparkDiary-1.0-SNAPSHOT-jar-with-dependencies.jar

 *
 * 结论：
 * 在本地测试成功将“大数据”和“十九大”分词，在yarn模式下不能识别“大数据”和“十九大”。
 *
 */
object AnsjLoadDicDemo1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {


    SetLogger

    val sparkConf = new SparkConf().setAppName(s"AnsjLoadDicDemo1").//setMaster("local[*]").
      set("spark.executor.memory", "2g").
      set("spark.Kryoserializer.buffer.max", "2048mb")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df = sc.parallelize(Seq(
      ("a", "MMLSpark", "MMLSpark：微软开源的用于Spark的深度学习库"),
      ("b", "数据可视化", "大数据时代当城市数据和社会关系被可视化，每个人都可能是福尔摩斯"),
      ("c", "十九大", "【权威发布】中国共产党第十九届中央委员会候补委员名单"))).
      toDF("id", "title", "content")
    df.show()

    // load stop words
    val stopwordsFile = "/personal/sunlu/lulu/yeeso/Stopwords.dic"
//    val stopwordsFile = "file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/Stopwords.dic"
    val stopwords = sc.textFile(stopwordsFile).collect().toList

    //定义UDF
    //分词、词性过滤

    def getKeyWordsFunc(title: String, content: String): String = {
      //每篇文章进行分词
      val segContent = title + " " + content

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
      filter($"getKW" =!= "NULL").withColumn("words", explode(split($"getKW", " ")))
    df1.show(100, false)

    sc.stop()
    spark.stop()

  }
}
