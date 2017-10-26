package com.evayInfo.Inglory.NLP.Ansj

import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/10/25.
 *
 * AnsjDemo4集群测试版本
 */
object AnsjDemo5 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    val sparkConf = new SparkConf().setAppName(s"AnsjDemo5").//setMaster("local[*]").
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
    val stopwords = sc.textFile(stopwordsFile).collect().toList

    // load user defined dic
    val userDefineFile = "/personal/sunlu/ylzx/userDefine.dic"
    //    val userDefineFile = "/personal/sunlu/NLP/userDic_20171024.txt"
    val userDefineList = sc.textFile(userDefineFile).collect().toList

    /*
    userDefineList.foreach(x => {
      UserDefineLibrary.insertWord(x, "userDefine", 1000)
    })
     */
//        MyStaticValue.userLibrary = "/root/software/NLP/userDic_20171024.txt"// bigdata7路径


//    UserDefineLibrary.contains("十九大")
//    UserDefineLibrary.loadLibrary(UserDefineLibrary.FOREST,"/root/software/NLP/userDic_20171024.txt")
    //http://blog.csdn.net/jo_joo/article/details/53689710
//    UserDefineLibrary.removeWord("十九大")

    //----添加自定义词典----
//    val dicfile = raw"/root/software/NLP/userDic_20171024.txt" //ExtendDic为一个文本文件的名字，里面每一行存放一个词
//    for (word <- Source.fromFile(dicfile).getLines) {
//      UserDefineLibrary.insertWord(word, "userDefine", 1000)
//    }

//    var forest:Forest = null
//    forest = Library.makeForest(WordSegmentTest.class.getResourceAsStream("/library/userLibrary.dic"))//加载字典文件



    //定义UDF
    //分词、词性过滤

    def getKeyWordsFunc(title: String, content: String): String = {
      //每篇文章进行分词
      val segContent = title + " " + content

      // Insert user defined words
            userDefineList.foreach(x => {
              UserDefineLibrary.insertWord(x, "userDefine", 1000)
            })

      //      MyStaticValue.userLibrary = "/root/lulu/Progect/NLP/userDic_20171024.txt"
      //----添加自定义词典----
//      val dicfile = raw"/root/lulu/Progect/NLP/userDic_20171024.txt" //ExtendDic为一个文本文件的名字，里面每一行存放一个词
//      for (word <- Source.fromFile(dicfile).getLines) {
//        UserDefineLibrary.insertWord(word, "userDefine", 1000)
//      }
//      MyStaticValue.userLibrary = "/root/software/NLP/userDic_20171024.txt"// bigdata7路径
//      println(UserDefineLibrary.contains("十九大"))
//      UserDefineLibrary.insertWord("十九大", "userDefine", 1000)
//      UserDefineLibrary.insertWord("大数据", "userDefine", 1000)
//      UserDefineLibrary.loadLibrary(UserDefineLibrary.FOREST,"/root/software/NLP/userDic_20171024.txt")
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
