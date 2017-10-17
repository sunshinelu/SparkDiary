package com.evayInfo.Inglory.SparkDiary.database.hbase

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by sunlu on 17/10/9.
 * 查看yilan-total-analysis_webpage表
 */
object checkAnalysisData {

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

  case class YlzxSchema2(itemString: String, title: String, manuallabel: String, time: String, timeL: Long, websitename: String, content: String, columnId: String)


  def getYlzxYRDD2(ylzxTable: String, year: Int, sc: SparkContext): RDD[YlzxSchema2] = {
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
    val N = year
    //  cal.add(Calendar.DATE, -N)//获取N天前或N天后的时间，-2为2天前
    cal.add(Calendar.YEAR, -N) //获取N年或N年后的时间，-2为2年前
    //    cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前

    val nDaysAgo = dateFormat.format(cal.getTime())
    val nDaysAgoL = dateFormat.parse(nDaysAgo).getTime

    //    @transient
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t")) //title
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //label
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("mod")) //time
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("websitename")) //websitename
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("c")) //content
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("column_id")) //column_id
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("sfzs")) //sfzs是否展示：1表示展示，0表示不展示
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("sfcj")) //sfcj是否采集：1表示重复采集，0表示未重复采集
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val title = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("t")) //标题列
      val manuallabel = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //标签列
      val time = v.getValue(Bytes.toBytes("f"), Bytes.toBytes("mod")) //时间列
      val webName = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("websitename")) //websitename列
      val content = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("c")) //content列
      val column_id = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("column_id")) // column_id列
      val sfzs = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("sfzs")) //sfzs
      val sfcj = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("sfcj")) //sfcj
      (urlID, title, manuallabel, time, webName, content, column_id, sfzs, sfcj)
    }
    }.filter(x => null != x._2 & null != x._3 & null != x._4 & null != x._5 & null != x._6).
      map { x => {
        val urlID_1 = Bytes.toString(x._1)
        val title_1 = if (null != x._2) Bytes.toString(x._2) else ""
        val manuallabel_1 = if (null != x._3) Bytes.toString(x._3) else ""
        //时间格式转化
        val time = Bytes.toLong(x._4)

        val websitename_1 = if (null != x._5) Bytes.toString(x._5) else ""
        val content_1 = Bytes.toString(x._6)
        val column_id = if (null != x._7) Bytes.toString(x._7) else ""
        val sfzs = if (null != x._7) Bytes.toString(x._7) else ""
        val sfcj = if (null != x._8) Bytes.toString(x._8) else ""
        (urlID_1, title_1, manuallabel_1, time, websitename_1, content_1, column_id, sfzs, sfcj)
      }
      }.filter(_._2.length >= 2).filter(_._8 != "0").filter(_._9 != "1").
      filter(x => x._4 >= nDaysAgoL).map(x => {
      //x._4 <= todayL &
      val date: Date = new Date(x._4)
      val time = dateFormat.format(date)
      val content = x._6.replace("&nbsp;", "").replaceAll("\\uFFFD", "").replaceAll("([\\ud800-\\udbff\\udc00-\\udfff])", "")
      YlzxSchema2(x._1, x._2, x._3, time, x._4, x._5, content, x._7)
    })

    hbaseRDD

  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName(s"checkAnalysisData").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val tableName = "yilan-total-analysis_webpage"

    val ylzxRDD = getYlzxYRDD2(tableName, 20, sc)
    val ylzxDS = spark.createDataset(ylzxRDD).dropDuplicates(Array("title", "time", "columnId")).drop("columnId")
    ylzxDS.select("itemString", "columnId").show(6, false)
    ylzxDS.filter($"columnId" =!= "NULL").select("itemString", "columnId").show(6, false)
    ylzxDS.filter($"itemString".contains("edf89aae-e381-4203-b77b-a137a1a57968")).
      select("itemString", "columnId").show(6, false)


    val test1 = ylzxDS.filter($"columnId" === "NULL")
    test1.show(5, false)
    println("the number of null in columnId is: " + test1.count())

    val test2 = ylzxDS.filter($"itemString" === "edf89aae-e381-4203-b77b-a137a1a57968")
    test2.select("itemString", "columnId").show(false)

    val test3 = ylzxDS.filter($"columnId" === "440b42e6-6029-49ae-bb0a-2b242e85cb54")
    test3.select("itemString", "columnId").show(false)

    val test4 = ylzxDS.filter($"columnId".contains("440b42e6-6029-49ae-bb0a-2b242e85cb54"))
    test4.select("itemString", "columnId").show(false)

    val test5 = ylzxDS.filter($"title".contains("从农业大数据看农业未来发展新方向"))
    test5.select("itemString", "title").show(false)

    sc.stop()
    spark.stop()

  }
}
