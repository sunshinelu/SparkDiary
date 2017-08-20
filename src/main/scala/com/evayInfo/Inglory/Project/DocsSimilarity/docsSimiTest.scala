package com.evayInfo.Inglory.Project.DocsSimilarity

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.io.Text
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{StringIndexer, Word2VecModel}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sunlu on 17/8/20.
 */
object docsSimiTest {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  case class YlzxSchema(urlID: String, title: String, content: String, label: String, time: String, websitename: String,
                        segWords: Seq[String])

  case class docSimsSchema(doc1: String, doc2: String, sims: Double)

  def getYlzxRDD(ylzxTable: String, sc: SparkContext): RDD[YlzxSchema] = {

    //load stopwords file
    val stopwordsFile = "/personal/sunlu/lulu/yeeso/Stopwords.dic"
    //    val stopwords = sc.textFile(stopwordsFile).collect().toList
    val stopwords = sc.broadcast(sc.textFile(stopwordsFile).collect().toList)

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

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("urlid")) //urlID
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("title")) //title
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content")) //content
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("label")) //label
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("time")) //label
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("websitename")) //websitename

    // scan.setTimeRange(1400468400000L, 1400472000000L)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取亿搜数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val urlID = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("urlid")) //标题列
      val title = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("title")) //内容列
      val content = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("content")) //标签列
      val label = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("label")) //网站名列
      val time = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("time")) //时间列
      val websitename = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("websitename")) //appc
      (rowkey, urlID, title, content, label, time, websitename)
    }
    }.filter(x => null != x._2 & null != x._3 & null != x._5 & null != x._6 & null != x._7).
      map { x => {
        val rowkey_1 = Bytes.toString(x._1)
        val urlID_1 = Bytes.toString(x._2)
        val title_1 = Bytes.toString(x._3)
        val content_1 = Bytes.toString(x._4)
        val label_1 = Bytes.toString(x._5)
        //时间格式转化
        val time_1 = Bytes.toLong(x._6)
        val websitename_1 = Bytes.toString(x._7)

        (urlID_1, title_1, content_1, label_1, time_1, websitename_1)
      }
      }.filter(x => x._2.length > 1 & x._3.length > 50).filter(x => x._5 <= todayL & x._5 >= nDaysAgoL).
      map(x => {
        val date: Date = new Date(x._5)
        val time = dateFormat.format(date)
        //使用ansj分词
        val segWords = ToAnalysis.parse(x._3).toArray.map(_.toString.split("/")).
          filter(_.length >= 2).map(_ (0)).toList.
          filter(word => word.length >= 2 & !stopwords.value.contains(word)).toSeq

        YlzxSchema(x._1, x._2, x._3, x._4, time, x._6, segWords)
      }) //.filter(x => null != x.segWords) //.filter(_.segWords.size > 1)//.randomSplit(Array(0.1,0.9))(0)

    hbaseRDD

  }

  def main(args: Array[String]) {
    //    SetLogger

    val sparkConf = new SparkConf().setAppName(s"docsSimiTest") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // load word2Vec model
    val word2VecModel = Word2VecModel.load("/personal/sunlu/Project/docsSimi/t_Word2VecModelDF")

    /*
     //load stopwords file
     val stopwordsFile = "/personal/sunlu/lulu/yeeso/Stopwords.dic"
     //    val stopwords = sc.textFile(stopwordsFile).collect().toList
     val stopwords = sc.broadcast(sc.textFile(stopwordsFile).collect().toList)
      */

    val ylzxTable = args(0)
    //    val ylzxTable = "t_ylzx_sun"
    val docSimiTable = args(1)
    val ylzxRDD = getYlzxRDD(ylzxTable, sc).repartition(100)
    val ylzxDS = spark.createDataset(ylzxRDD)

    val indexer = new StringIndexer()
      .setInputCol("urlID")
      .setOutputCol("id")

    val indexedDF = indexer.fit(ylzxDS).transform(ylzxDS)
    //    indexedDF.persist(StorageLevel.MEMORY_AND_DISK)

    /*
    //定义UDF
    //分词、停用词过滤

    def segWordsFunc(content: String): Seq[String] = {
      val seg = ToAnalysis.parse(content).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 1 & !stopwords.value.contains(word)).toSeq
      val result = seg match {
        case r if (r.length >= 1) => r
        case _ => Seq("null")
      }
      result
    }
    val segWords2 = udf((content: String) => segWordsFunc(content).mkString(" "))


    val segWords = udf((content: String) => {
      ToAnalysis.parse(content).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 1 & !stopwords.value.contains(word)).toSeq
    })


    val segDF = indexedDF.withColumn("segWordsTemp", segWords2(column("content"))).
      filter(! col("segWordsTemp").contains("null")).withColumn("segWords", explode(split(col("segWordsTemp"), " "))).
      drop("segWordsTemp")
*/

    /*
    val word2Vec = new Word2Vec()
      .setInputCol("segWords")
      .setOutputCol("features")
      .setVectorSize(3) // 1000
      .setMinCount(1)
    val word2VecModel = word2Vec.fit(indexedDF)
    word2VecModel.write.overwrite().save("/personal/sunlu/Project/docsSimi/t_Word2VecModelDF")
*/
    val word2VecDF = word2VecModel.transform(indexedDF)
    val document = word2VecDF.select("id", "features").na.drop.rdd.map {
      case Row(id: Double, features: MLVector) => (id.toLong, Vectors.fromML(features))
    }.filter(_._2.size >= 2) //.distinct()//.repartition(300)

    val tfidf = document.map { case (i, v) => new IndexedRow(i, v) } //.repartition(50)
    val mat = new IndexedRowMatrix(tfidf)

    val transposed_matrix = mat.toCoordinateMatrix.transpose()
    /* The minimum cosine similarity threshold for each document pair */
    val threshhold = 0.7.toDouble
    val upper = 1.0

    val sim = transposed_matrix.toRowMatrix.columnSimilarities(threshhold)
    //    val exact = transposed_matrix.toRowMatrix.columnSimilarities()

    val sim_threshhold = sim.entries.filter { case MatrixEntry(i, j, u) => u >= threshhold && u <= upper } //.repartition(400)
    //    sim_threshhold.persist(StorageLevel.MEMORY_AND_DISK)

    val docSimsRDD = sim_threshhold.map { x => {
      val doc1 = x.i.toString
      val doc2 = x.j.toString
      val sims = x.value.toDouble
      //保留sims中有效数字
      val sims2 = f"$sims%1.5f".toDouble
      docSimsSchema(doc1, doc2, sims2)
    }
    }

    //    sim_threshhold.unpersist()

    val docSimsDS = spark.createDataset(docSimsRDD) //.repartition(50)

    //对dataframe进行分组排序，并取每组的前5个
    //计算两个向量的余弦相似度，值越大就表示越相似。
    val w = Window.partitionBy("doc1").orderBy(col("sims").desc)
    val ds5 = docSimsDS.withColumn("rn", row_number.over(w)).where(col("rn") <= 5)

    val wordsUrlLabDF = indexedDF.withColumnRenamed("id", "doc2").withColumnRenamed("urlID", "url2id").
      select("doc2", "url2id", "title", "label", "time", "websitename")

    val wordsUrlLabDF2 = indexedDF.withColumnRenamed("id", "doc1").select("doc1", "urlID")
    //    indexedDF.unpersist()

    val ds6 = ds5.join(wordsUrlLabDF, Seq("doc2"), "left")
    //doc1,doc2,sims,rn,url2id,title2,label2,time2,websitename2
    val ds7 = ds6.join(wordsUrlLabDF2, Seq("doc1"), "left").na.drop()
    //doc1,doc2,sims,rn,url2id,title2,label2,time2,websitename2,urlID

    // doc1, doc2, sims, rn, url2id,  title, label, time, websitename, urlID


    val hbaseConf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    hbaseConf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名

    /*
    //如果outputTable存在则不做任何操作，如果HBASE表不存在则新建表
    val hadmin = new HBaseAdmin(hbaseConf)
    if (!hadmin.isTableAvailable(docSimiTable)) {
      print("Table Not Exists! Create Table")
      val tableDesc = new HTableDescriptor(TableName.valueOf(docSimiTable))
      tableDesc.addFamily(new HColumnDescriptor("info".getBytes()))
//      tableDesc.addFamily(new HColumnDescriptor("f".getBytes()))
      hadmin.createTable(tableDesc)
    }else{
      print("Table  Exists!  not Create Table")
    }
*/

    //如果outputTable表存在，则删除表；如果不存在则新建表。=> START
    val hAdmin = new HBaseAdmin(hbaseConf)
    if (hAdmin.tableExists(docSimiTable)) {
      hAdmin.disableTable(docSimiTable)
      hAdmin.deleteTable(docSimiTable)
    }
    //    val htd = new HTableDescriptor(outputTable)
    val htd = new HTableDescriptor(TableName.valueOf(docSimiTable))
    htd.addFamily(new HColumnDescriptor("info".getBytes()))
    hAdmin.createTable(htd)
    //如果outputTable表存在，则删除表；如果不存在则新建表。=> OVER

    //指定输出格式和输出表名
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, docSimiTable) //设置输出表名


    //    val table = new HTable(hbaseConf,docSimiTable)
    //    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE,docSimiTable)

    val jobConf = new Configuration(hbaseConf)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)


    ds7.repartition(10).rdd.map(row => (row(2), row(3), row(4), row(5), row(6), row(7), row(8), row(9))).
      map { x => {
        //sims, rn, url2id,  title, label, time, websitename, urlID
        val paste = x._8 + "::score=" + x._2.toString
        val key = Bytes.toBytes(paste)
        val put = new Put(key)
        put.add(Bytes.toBytes("info"), Bytes.toBytes("simsScore"), Bytes.toBytes(x._1.toString)) //标签的family:qualify,sims
        put.add(Bytes.toBytes("info"), Bytes.toBytes("level"), Bytes.toBytes(x._2.toString)) //rn
        put.add(Bytes.toBytes("info"), Bytes.toBytes("simsID"), Bytes.toBytes(x._3.toString)) //url2id
        put.add(Bytes.toBytes("info"), Bytes.toBytes("t"), Bytes.toBytes(x._4.toString)) //title2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("manuallabel"), Bytes.toBytes(x._5.toString)) //label2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("mod"), Bytes.toBytes(x._6.toString)) //time2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("websitename"), Bytes.toBytes(x._7.toString)) //websitename2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(x._8.toString)) //urlID
        (new ImmutableBytesWritable, put)
      }
      }.saveAsNewAPIHadoopDataset(jobConf)

    sc.stop()
    spark.stop()
  }
}
