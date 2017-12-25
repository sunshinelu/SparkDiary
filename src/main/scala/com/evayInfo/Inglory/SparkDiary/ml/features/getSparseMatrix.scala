package com.evayInfo.Inglory.SparkDiary.ml.features

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{HashingTF, IDF, Word2Vec}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sunlu on 17/12/24.
 * 读取hbase中的p:c列，分词后计算tf-idf，将稀疏矩阵结果保存到本地
 *  * spark-shell下测试：
 * spark-shell --master yarn --num-executors 8 --executor-cores  4 --executor-memory 8g --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar

 */
object getSparseMatrix {

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  case class HBaseSchema(itemString: String, content: String)


  def getHBaseRDD(ylzxTable: String, sc: SparkContext): RDD[HBaseSchema] = {
    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd

    //获取当前时间
    val now: Date = new Date()
    //对时间格式尽心格式化
    val today = dateFormat.format(now)
    //把时间转换成long类型
    val todayL = dateFormat.parse(today).getTime
    //获取N天的时间，并把时间转换成long类型
    val cal: Calendar = Calendar.getInstance()
    val N = 1
    //  cal.add(Calendar.DATE, -N)//获取N天前或N天后的时间，-2为2天前
    cal.add(Calendar.YEAR, -N) //获取N年或N年后的时间，-2为2年前
    //    cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前

    val nDaysAgo = dateFormat.format(cal.getTime())
    val nDaysAgoL = dateFormat.parse(nDaysAgo).getTime

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("mod")) //time
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t")) //content
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val time = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("mod")) //时间列
      val content = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t")) //content列
      (urlID, time, content)
    }
    }.filter(x => null != x._2 & null != x._3).
      map { x => {
        val urlID_1 = Bytes.toString(x._1)
        //时间格式转化
        val time = Bytes.toLong(x._2)
        val content_1 = Bytes.toString(x._3)
        (urlID_1, time, content_1)
      }
      }.filter(x => x._2 <= todayL & x._2 >= nDaysAgoL & x._3.length >= 5).map(x => {
      val date: Date = new Date(x._2)
      val time = dateFormat.format(date)
      val content = x._3.replace("&nbsp;", "").replaceAll("\\uFFFD", "").replaceAll("([\\ud800-\\udbff\\udc00-\\udfff])", "")
      HBaseSchema(x._1, content)
    })
    hbaseRDD
  }

  def main(args: Array[String]) {
    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"getSparseMatrix").setMaster("local[*]").set("spark.executor.memory", "4g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val ylzxTable = "yilan-total-analysis_webpage"

    val stopwordsFile = "/personal/sunlu/lulu/yeeso/Stopwords.dic"
    //    val stopwords = sc.textFile(stopwordsFile).collect().toList
    val stopwords = sc.broadcast(sc.textFile(stopwordsFile).collect().toList)

    val ylzxRDD = getHBaseRDD(ylzxTable, sc)
    val ylzxDS = spark.createDataset(ylzxRDD).limit(100000)


    //定义UDF
    //分词、停用词过滤
    def segWordsFunc(content: String): Seq[String] = {
      val seg = ToAnalysis.parse(content).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 1).toSeq
      val result = seg match {
        case r if (r.length >= 1) => r
        case _ => Seq("") // Seq("null")
      }
      result
    }
    val segWordsUDF = udf((content:String) => segWordsFunc(content))

    val segDF = ylzxDS.withColumn("segWords", segWordsUDF($"content"))

    /*
calculate tf-idf value
*/
    val hashingTF = new HashingTF().
      setInputCol("segWords").
      setOutputCol("rawFeatures").
      setNumFeatures(1024)

    val featurizedData = hashingTF.transform(segDF)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().
      setInputCol("rawFeatures").
      setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val tfidfData = idfModel.transform(featurizedData)
    /*
    val sparseMatrix = tfidfData.select("features").
      rdd.map{case Row(features:MLVector) => Vectors.fromML(features)}
    sparseMatrix.saveAsTextFile("/personal/sunlu/Matrix/SparseMatrix")
*/

    val denseMatrix = tfidfData.select("itemString","features").
      rdd.map{case Row(id:String,features:MLVector) => (id,Vectors.fromML(features).toDense)}
    denseMatrix.persist()
    denseMatrix.map(_._2).saveAsTextFile("/personal/sunlu/Matrix/DenseMatrix_1W_1024_matrix")
    denseMatrix.saveAsTextFile("/personal/sunlu/Matrix/DenseMatrix_1W_1024")



    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec().
    setInputCol("segWords").
    setOutputCol("word2vecFeatures").
    setVectorSize(1024). // 1000
    setMinCount(2)
    val word2VecModel = word2Vec.fit(segDF)
    val word2VecDF = word2VecModel.transform(segDF)
    val document = word2VecDF.select("itemString", "word2vecFeatures").na.drop.rdd.map {
      case Row(id: String, features: MLVector) => (id, Vectors.fromML(features).toDense)
    }
    document.persist()
    document.saveAsTextFile("/personal/sunlu/Matrix/DenseMatrix_1W_1024")
    document.map(_._2).saveAsTextFile("/personal/sunlu/Matrix/DenseMatrix_1W_1024_matrix")



    sc.stop()
    spark.stop()
  }

}
