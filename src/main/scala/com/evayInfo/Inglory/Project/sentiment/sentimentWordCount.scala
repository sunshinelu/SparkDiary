package com.evayInfo.Inglory.Project.sentiment

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.evayInfo.Inglory.util.{timeSeries, truncateMysql}
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 17/7/14.
 * * Created by sunlu on 17/6/5.
 * 使用word count的方法统计正类、负类情感词的个数
 *
 * article(文章表):
  id varchar(50) CHARACTER SET utf8 DEFAULT NULL,
  title: 标题
  content: 内容
  author: 作者
  time: 发布时间
  clicknum: 点击数
  reply: 回复数
 *
 *
 * comment(评论表):
  id varchar(50) CHARACTER SET utf8 DEFAULT NULL,
  articleid: 对应文章表中的文章id',
  jsusername: 评论作者
  jsrestime: 评论时间
  floorid: 楼
  bbscontent: 评论的内容
 *
 * replycomment(对评论的回复):
  id varchar(50) CHARACTER SET utf8 DEFAULT NULL,
  commentid: 对应评论表中的id
  username: 回复的作者
  replytime: 回复的时间
  ircontent: 回复的内容

 *
 *
 * 本地运行成功
 */
object sentimentWordCount {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }

  case class Book(title: String, words: String)

  case class Corpus(id: String, content: String, segWords: String, time: String, tokens: String)

  case class mysqlSchema(date: String, lable: String, value: Double, sysTime: String)

  def main(args: Array[String]) {

    SetLogger

    val conf = new SparkConf().setAppName(s"sentimentWordCount").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //获取正类、负类词典。posnegDF在join时使用；posnegList在词过滤时使用。
    val posnegDF = spark.read.format("CSV").option("header", "true").load("data/sentiment/posneg.csv")

    val posnegList = posnegDF.select("term").dropDuplicates().rdd.map { case Row(term: String) => term }.collect().toList

    //    posnegList.collect().take(10).foreach(println)

    //    posnegDF.show(5)

    //load stopwords file
    val stopwordsFile = "data/Stopwords.dic"
    val stopwords = sc.textFile(stopwordsFile).collect().toList

    //connect mysql database
    val url1 = "jdbc:mysql://localhost:3306/bbs"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")
    //get data
    val ds1 = spark.read.jdbc(url1, "article", prop1)
    // ds1.show(5)

    //定义UDF
    //分词、停用词过滤、正类、负类词过滤
    val segWorsd = udf((content: String) => {
      ToAnalysis.parse(content).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 1 & !stopwords.contains(word)).filter(word => posnegList.contains(word))
        .toSeq.mkString(" ")
    })
    //时间处理
    val replaceString = udf((content: String) => {
      content.replace("时间：", "").split(" ")(0)
    })

    /*
    //调整时间格式
    def timeFormat(time: String): String = {
      //定义时间格式
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")// yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
      val dateFormat2 = new SimpleDateFormat("yyyy-MM-dd")// yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
      val creatTimeD = dateFormat.parse(time)
      val creatTimeS = dateFormat2.format(creatTimeD)
      //val creatTimeL = dateFormat2.parse(creatTimeS).getTime
      creatTimeS
    }

//这个UDF在dataframe中不能用
    val timeFormat = udf((time: String) => {
      //定义时间格式
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")// yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
      val dateFormat2 = new SimpleDateFormat("yyyy-MM-dd")// yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
      val creatTimeD = dateFormat.parse(time)
      val creatTimeS = dateFormat2.format(creatTimeD)
      //val creatTimeL = dateFormat2.parse(creatTimeS).getTime

    })


*/
    //
    val ds2 = ds1.na.drop(Array("content")).select("id", "content", "time").withColumn("segWords", segWorsd(column("content"))).
      withColumn("time2", replaceString(col("time"))).drop("time")
    //    ds2.show(5)
    //    ds2.printSchema()

    //  val ds2_2 = ds2.withColumn("timeDay", timeFormat(col("time2")))//报错！如何定义在dataframe中使用的function


    val ds3 = ds2.explode("segWords", "word") { segWords: String => segWords.split(" ") }
    //    ds3.show(5)


    val ds4 = ds2.as[(String, String, String, String)]
    val ds5 = ds4.flatMap {
      case (x1, x2, x3, x4) => x3.split(" ").map(Corpus(x1, x2, x3, x4, _))
    }.toDF
    //    ds5.show(5)//ds5和ds3一样
    //    ds5.printSchema()

    val ds6 = ds5.join(posnegDF, ds5("tokens") === posnegDF("term"), "left").na.drop()

    //    ds6.show(5)

    val ds7 = ds6.groupBy("id", "time").agg(sum("weight")).withColumnRenamed("sum(weight)", "value")
    //    ds7.printSchema()
    //    ds7.show(5)
    //    println(ds7.count())
    //    println("unique id number is: " + ds7.dropDuplicates("id").count)
    //    println("unique time number is " + ds7.dropDuplicates("time").count)


    val sentimentLabel = udf((value: Int) => {
      value match {
        case x if x > 0 => "正类"
        case x if x < 0 => "负类"
        case _ => "中性"
      }
    })

    val ds8 = ds7.withColumn("label", sentimentLabel(col("value")))
    //    ds8.printSchema()
    //    ds8.show(10)

    val ds9 = ds8.groupBy("time", "label").agg(count("label")).withColumnRenamed("count(label)", "value")
    //    ds9.printSchema()
    //    ds9.show(5)
    //    println(ds9.count)

    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)
    val addSysTime = udf((userString: String) => {
      today
    })
    val ds10 = ds9.withColumn("CREATETIME", addSysTime(col("time")))
    //    ds10.printSchema()
    //    ds10.show(10)
    val columnsRenamed = Seq("time", "label", "value", "sysTime")
    val df = ds10.toDF(columnsRenamed: _*)
    //    df.printSchema()
    //    df.show(10)
    //    println(df.count())

    /*
    val n = ds10.count().toInt
    val id = Seq((1 to n))

    val idDF = df.select("channelC").as("id")

    val df2 = df.union(df)
    df2.printSchema()
    df2.show()
    println(df2.count())
*/
    /*
        df.toDF().toJSON.take(2).foreach(println)
        println("=======")

        df.toJSON.take(2).foreach(println)

        println("=======")

        df.toJSON.collect.foreach(println)
        */
    val labelUdf = udf((arg: String) => {
      "正类,负类,中性"
    })
    //获取时间差
    val n = 10

    val tsDF = sc.parallelize(timeSeries.gTimeSeries(15)).toDF("time").
      withColumn("labels", labelUdf($"time")).as[(String, String)]
    val tsDF_2ColumnsName = Seq("time", "label")
    val tsDF_2 = tsDF.flatMap {
      case (timeSeries, labels) => labels.split(",").map((timeSeries, _))
    }.toDF(tsDF_2ColumnsName: _*)
    //对指定的列空值填充
    val df2 = tsDF_2.join(df, Seq("time", "label"), "left").na.fill(value = 0, cols = Array("value")).
      withColumn("value", $"value".cast("int")).drop("sysTime").withColumn("sysTime", current_timestamp())
    //    df2.printSchema()
    //    df2.show(10)


    //将df4保存到hotWords_Test表中
    val url2 = "jdbc:mysql://localhost:3306/bbs?useUnicode=true&characterEncoding=UTF-8"
    //使用"?useUnicode=true&characterEncoding=UTF-8"以防止出现存入MySQL数据库中中文乱码情况
    val prop2 = new Properties()
    prop2.setProperty("user", "root")
    prop2.setProperty("password", "root")
    //清空sentiment_wc表
    truncateMysql.truncateMysql("jdbc:mysql://localhost:3306/bbs", "root", "root", "sentiment_wc")

    //将结果保存到数据框中
    df2.write.mode("append").jdbc(url2, "sentiment_wc", prop2) //overwrite or append

    println("run succeeded!")

    sc.stop()
    spark.stop()


    /*
       val dfList = List(("Hadoop", "Java,SQL,Hive,HBase,MySQL"), ("Spark", "Scala,SQL,DataSet,MLlib,GraphX"))

       val df = dfList.map{p=>Book(p._1,p._2)}.toDS()

       df.flatMap(_.words.split(","))//.show

    */


  }
}
