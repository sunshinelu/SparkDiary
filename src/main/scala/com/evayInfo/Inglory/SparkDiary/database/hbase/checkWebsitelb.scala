package com.evayInfo.Inglory.SparkDiary.database.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.io.Text
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
/*
查看total表中的p:websitelb
 */

object checkWebsitelb {

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
  case class LabelView(itemString: String, webLabel:String)

  def main(args: Array[String]): Unit = {

    SetLogger
    val sparkConf = new SparkConf().setAppName(s"swt: checkWebsitelb").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val ylzxTable = "yilan-total_webpage"
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("websitelb")) //网站类别

    // scan.setTimeRange(1400468400000L, 1400472000000L)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val webLabelRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val webLabel = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("websitelb")) //网站类别
      (urlID, webLabel)
    }
    }.filter(x => null != x._2).
      map(x => {
        val urlID_1 = Bytes.toString(x._1)
        val webLabel_1 = Bytes.toString(x._2)
        LabelView(urlID_1, webLabel_1)
      }
      ).filter(_.webLabel.length >= 2)

    val webLabelDf = spark.createDataset(webLabelRDD)
//    println("webLabelDf的数量为：" + webLabelDf.count())
//webLabelDf的数量为：129825

    def filterChinese(input:String):String = {
      val regx ="""[\u4e00-\u9fa5]+""".r
      val output = regx.replaceAllIn(input, "汉字")
      return output
    }
    val filterChineseUDF = udf((col: String) => filterChinese(col))

    val df1 = webLabelDf.withColumn("whether", filterChineseUDF($"webLabel")).filter($"whether".contains("汉字"))
//    println("p:websitelb列中为汉字的数量为：" + df1.count())
    //p:websitelb列中为汉字的数量为：44088


    def filterNumber(input:String):String = {
     val regx = """[0-9]+""".r
      val output = regx.replaceAllIn(input, "数字")
      return output
    }
    val filterNumberUDF = udf((col: String) => filterNumber(col))
    val df2 = webLabelDf.withColumn("whether", filterNumberUDF($"webLabel")).filter($"whether".contains("数字"))
//    println("p:websitelb列中为数字的数量为：" + df2.count())
//p:websitelb列中为数字的数量为：85737

    val dic = spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").
      option("header", true).option("delimiter", ",").
      load("file:///D:\\Workspace\\IDEA\\GitHub\\SparkDiary\\data\\webLab.csv").
      withColumnRenamed("TEXT", "webLabel")

    val df3 = df1.join(dic, Seq("webLabel"), "left")
//    println("df3的数量为：" + df3.count())
    //df3的数量为：44088
    //    df3.printSchema()
    //    df3.show(5)

    val df4_rdd = df3.select("itemString", "VALUE").rdd.map{case(rowkey:String, websitelb:String) => (rowkey, websitelb)}

    //指定输出格式和输出表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, ylzxTable) //设置输出表名

    val jobConf = new Configuration(conf)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)

    df4_rdd.map(x => {
      val key = Bytes.toBytes(x._1)
      val put = new Put(key)
      put.add(Bytes.toBytes("p"), Bytes.toBytes("websitelb"), Bytes.toBytes(x._2))
      (new ImmutableBytesWritable, put)
    }).saveAsNewAPIHadoopDataset(jobConf)

    sc.stop()
    spark.stop()


  }
}
