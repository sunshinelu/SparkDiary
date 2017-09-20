package com.evayInfo.Inglory.SparkDiary.database.hbase

import java.text.SimpleDateFormat

import com.evayInfo.Inglory.Project.Recommend.UtilTool
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sunlu on 17/9/20.
  */
object getHotLabe {

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

  def main(args: Array[String]) {

    SetLogger

    val SparkConf = new SparkConf().setAppName(s"getHotLabe").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val logsTable = "t_hbaseSink"
    //(userString: String, searchWords: String, searchValue: Double)
    val hotlabelRDD = getHotLabelLogRDD(logsTable, sc)

//    println("hotlabelRDD is: " + hotlabelRDD.count())
    //hotlabelRDD is: 31518

    val hotlabelDS = spark.createDataset(hotlabelRDD).na.drop(Array("userString")).
      groupBy("userString", "searchWords").
      agg(sum("searchValue")).
      withColumnRenamed("sum(searchValue)", "value").drop("searchValue")

//    println("hotlabelDS is: " + hotlabelDS.count())
    //hotlabelDS is: 364

    val hotlabel_df1 = hotlabelDS.withColumnRenamed("userString", "OPERATOR_ID").
      withColumnRenamed("searchWords", "userFeature")

//    println("hotlabel_df1 is: " + hotlabel_df1.count())
    //hotlabel_df1 is: 364
//    hotlabel_df1.show(false)
    hotlabel_df1.select("userFeature").dropDuplicates().show(200,false)

    val testRDD = getTestRDD(logsTable, sc)
    val testDS = spark.createDataset(testRDD)


    sc.stop()
    spark.stop()

  }

  case class LogView(CREATE_BY_ID: String, CREATE_TIME: Long, REQUEST_URI: String, PARAMS: String)

  case class HotLabelSchema(userString: String, searchWords: String, CREATE_TIME: Long, value: Double)

  case class HotLabelSchema2(userString: String, searchWords: String, searchValue: Double)


  def getHotLabelLogRDD(logsTable: String, sc: SparkContext): RDD[HotLabelSchema2] = {
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, logsTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cREATE_BY_ID")) //cREATE_BY_ID
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cREATE_TIME")) //cREATE_TIME
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rEQUEST_URI")) //rEQUEST_URI
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("pARAMS")) //pARAMS
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val userID = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("cREATE_BY_ID")) //cREATE_BY_ID
      val creatTime = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("cREATE_TIME")) //cREATE_TIME
      val requestURL = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("rEQUEST_URI")) //rEQUEST_URI
      val parmas = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("pARAMS")) //pARAMS
      (userID, creatTime, requestURL, parmas)
    }
    }.filter(x => null != x._1 & null != x._2 & null != x._3 & null != x._4).
      map { x => {
        val userID = Bytes.toString(x._1)
        val creatTime = Bytes.toString(x._2)
        //定义时间格式
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
        val dateFormat2 = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
        val creatTimeD = dateFormat.parse(creatTime)
        val creatTimeS = dateFormat.format(creatTimeD)
        val creatTimeL = dateFormat2.parse(creatTimeS).getTime

        val requestURL = Bytes.toString(x._3)
        val parmas = Bytes.toString(x._4)
        LogView(userID, creatTimeL, requestURL, parmas)
      }
      }.filter(x => {
      x.REQUEST_URI.contains("search.do") && x.PARAMS.contains("manuallabel=") && x.CREATE_BY_ID.length >= 5
    }).map(x => {
      //hotlabelRDD is: 41931
      val userID = x.CREATE_BY_ID.toString
      //      val reg = """manuallabel=.+(,|})""".r
      //      val reg ="""manuallabel=.+,|manuallabel=.+}""".r
      val reg =
      """manuallabel=([\u4e00-\u9fa5]|[a-zA-Z])+""".r
      val searchWord = reg.findFirstIn(x.PARAMS.toString).toString.replace("Some(manuallabel=", "").replace(",)", "").replace("}", "").replace(")", "")
      val time = x.CREATE_TIME
      val value = 1.0
      HotLabelSchema(userID, searchWord, time, value) //hotlabelRDD is: 41931
    }).filter(x => (x.searchWords.length >= 2 && x.searchWords.length <= 10)).map(x => {
      //hotlabelRDD is: 31518
      val userString = x.userString
      val searchWords = x.searchWords
      val time = x.CREATE_TIME
      val value = x.value

      val rating = time match {
        case x if (x >= UtilTool.get3Dasys()) => 0.9 * value
        case x if (x >= UtilTool.get7Dasys() && x < UtilTool.get3Dasys()) => 0.8 * value
        case x if (x >= UtilTool.getHalfMonth() && x < UtilTool.get7Dasys()) => 0.7 * value
        case x if (x >= UtilTool.getOneMonth() && x < UtilTool.getHalfMonth()) => 0.6 * value
        case x if (x >= UtilTool.getSixMonth() && x < UtilTool.getOneMonth()) => 0.5 * value
        case x if (x >= UtilTool.getOneYear() && x < UtilTool.getSixMonth()) => 0.4 * value
        case x if (x < UtilTool.getOneYear()) => 0.3 * value
        case _ => 0.0
      }

      HotLabelSchema2(userString, searchWords, rating)
    })

    hbaseRDD
  }

  def getTestRDD(logsTable: String, sc: SparkContext): RDD[LogView] = {
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, logsTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cREATE_BY_ID")) //cREATE_BY_ID
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cREATE_TIME")) //cREATE_TIME
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rEQUEST_URI")) //rEQUEST_URI
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("pARAMS")) //pARAMS
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val userID = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("cREATE_BY_ID")) //cREATE_BY_ID
      val creatTime = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("cREATE_TIME")) //cREATE_TIME
      val requestURL = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("rEQUEST_URI")) //rEQUEST_URI
      val parmas = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("pARAMS")) //pARAMS
      (userID, creatTime, requestURL, parmas)
    }
    }.filter(x => null != x._1 & null != x._2 & null != x._3 & null != x._4).
      map { x => {
        val userID = Bytes.toString(x._1)
        val creatTime = Bytes.toString(x._2)
        //定义时间格式
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
        val dateFormat2 = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
        val creatTimeD = dateFormat.parse(creatTime)
        val creatTimeS = dateFormat.format(creatTimeD)
        val creatTimeL = dateFormat2.parse(creatTimeS).getTime

        val requestURL = Bytes.toString(x._3)
        val parmas = Bytes.toString(x._4)
        LogView(userID, creatTimeL, requestURL, parmas)
      }
      }.filter(x => {
      x.REQUEST_URI.contains("search.do") && x.PARAMS.contains("manuallabel=") &&
        x.CREATE_BY_ID.length >= 5 && x.PARAMS.contains("cuPage=")
    })
    /*.map(x => {
          //hotlabelRDD is: 41931
          val userID = x.CREATE_BY_ID.toString
          //      val reg = """manuallabel=.+(,|})""".r
          val reg =
            """manuallabel=.+,|manuallabel=.+}""".r
          val searchWord = reg.findFirstIn(x.PARAMS.toString).toString.replace("Some(manuallabel=", "").replace(",)", "").replace("}", "").replace(")", "")
          val time = x.CREATE_TIME
          val value = 1.0
          HotLabelSchema(userID, searchWord, time, value) //hotlabelRDD is: 41931
        }).filter(x => (x.searchWords.length >= 2 && x.searchWords.length <= 10)).map(x => {
          //hotlabelRDD is: 31518
          val userString = x.userString
          val searchWords = x.searchWords
          val time = x.CREATE_TIME
          val value = x.value

          val rating = time match {
            case x if (x >= UtilTool.get3Dasys()) => 0.9 * value
            case x if (x >= UtilTool.get7Dasys() && x < UtilTool.get3Dasys()) => 0.8 * value
            case x if (x >= UtilTool.getHalfMonth() && x < UtilTool.get7Dasys()) => 0.7 * value
            case x if (x >= UtilTool.getOneMonth() && x < UtilTool.getHalfMonth()) => 0.6 * value
            case x if (x >= UtilTool.getSixMonth() && x < UtilTool.getOneMonth()) => 0.5 * value
            case x if (x >= UtilTool.getOneYear() && x < UtilTool.getSixMonth()) => 0.4 * value
            case x if (x < UtilTool.getOneYear()) => 0.3 * value
            case _ => 0.0
          }

          HotLabelSchema2(userString, searchWords, rating)
        })
    */
    hbaseRDD
  }

}
