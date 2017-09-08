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
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sunlu on 17/9/8.
 */
object checkUserLogs {

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

  case class LogView(CREATE_BY_ID: String, CREATE_TIME_L: Long, CREATE_TIME: String, REQUEST_URI: String, PARAMS: String)

  case class LogView2(userString: String, itemString: String, CREATE_TIME: String, value: Double)


  def getLogsRDD(logsTable: String, sc: SparkContext): RDD[LogView2] = {

    // 获取时间
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
    //    cal.add(Calendar.YEAR, -N) //获取N年或N年后的时间，-2为2年前
    cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前
    val nDaysAgo = dateFormat.format(cal.getTime())
    val nDaysAgoL = dateFormat.parse(nDaysAgo).getTime


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
        val creatTimeS = dateFormat2.format(creatTimeD)
        val creatTimeL = dateFormat2.parse(creatTimeS).getTime

        val requestURL = Bytes.toString(x._3)
        val parmas = Bytes.toString(x._4)
        LogView(userID, creatTimeL, creatTimeS, requestURL, parmas)
      }
      }.filter(x => x.REQUEST_URI.contains("getContentById.do") || x.REQUEST_URI.contains("like/add.do") ||
      x.REQUEST_URI.contains("favorite/add.do") || x.REQUEST_URI.contains("favorite/delete.do") ||
      x.REQUEST_URI.contains("addFavorite.do") || x.REQUEST_URI.contains("delFavorite.do")
    ).
      filter(_.CREATE_TIME_L >= nDaysAgoL).
      filter(_.PARAMS.toString.length >= 10).
      map(x => {
        val userID = x.CREATE_BY_ID.toString
        //        val reg2 = """id=(\w+\.){2}\w+.*,""".r
        val reg2 =
          """id=\S*,|id=\S*}""".r
        val urlString = reg2.findFirstIn(x.PARAMS.toString).toString.replace("Some(id=", "").replace(",)", "").replace("})", "")
        val time = x.CREATE_TIME
        val value = 1.0
        val rating = x.REQUEST_URI match {
          case r if (r.contains("getContentById.do")) => 1.0 * value
          case r if (r.contains("like/add.do")) => 1.0 * value
          case r if (r.contains("favorite/add.do")) => 1.0 * value
          case r if (r.contains("addFavorite.do")) => 1.0 * value //0.5
          case r if (r.contains("favorite/delete.do")) => -1.0 * value
          case r if (r.contains("delFavorite.do")) => -1.0 * value //-0.5
          case _ => 0.0 * value
        }

        LogView2(userID, urlString, time, rating)
      }).filter(_.itemString.length >= 5).filter(_.userString.length >= 5)

    hbaseRDD
  }


  def getLogsRDD2(logsTable: String, sc: SparkContext): RDD[LogView] = {

    // 获取时间
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
    //    cal.add(Calendar.YEAR, -N) //获取N年或N年后的时间，-2为2年前
    cal.add(Calendar.MONTH, -N) //获取N月或N月后的时间，-2为2月前
    val nDaysAgo = dateFormat.format(cal.getTime())
    val nDaysAgoL = dateFormat.parse(nDaysAgo).getTime


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
        val creatTimeS = dateFormat2.format(creatTimeD)
        val creatTimeL = dateFormat2.parse(creatTimeS).getTime

        val requestURL = Bytes.toString(x._3)
        val parmas = Bytes.toString(x._4)
        LogView(userID, creatTimeL, creatTimeS, requestURL, parmas)
      }
      } //.filter(_.CREATE_TIME_L >= nDaysAgoL)

    hbaseRDD
  }

  case class LogView3(CREATE_BY_ID: String, REQUEST_URI: String, PARAMS: String)

  case class Schema1(userString: String, itemString: String)

  def main(args: Array[String]) {
    SetLogger
    val sparkConf = new SparkConf().setAppName(s"swt: checkWebsitelb").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val user_Y = sc.textFile("file:///Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/data/ylzx_user_y.txt").toDF("userId")
    val user_A = sc.textFile("file:///Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/data/ylzx_user_A.txt").toDF("userId")
    println("昨天访问的用户数量为：" + user_Y.count()) // 昨天访问的用户数量为：44
    println("近一月的访问的用户数量为：" + user_A.count()) //近一月的访问的用户数量为：22

    val myID = "175786f8-1e74-4d6c-94e9-366cf1649721"

    val df1 = user_Y.join(user_A, Seq("userId"), "leftanti")
    println("昨天新增的用户数量为：" + df1.count()) // 昨天新增的用户数量为：29
    //    df1.collect().foreach(println)
    /*
[1661350d-0bd6-42d1-9ca8-0d651b5e6b96]
[dab23789-5659-49e0-9a2c-d6d9d72a020d]
[48bacb50-d533-4f79-ae1c-05bda21b5e95]
[08c0956c-e02d-4c97-96e9-228564407814]
[17cad950-7bff-46cb-9da4-bd2637e91aed]
[6abe43c2-4925-4c7d-9515-db7b64bd814d]
[a4d38037-759b-4082-8b0c-943261bd4087]
[ec664f79-6cd9-4348-b578-cc4c00babc3c]
[60ecadca-4b6e-46f4-b3d2-2a00037aeddf]
[6324063a-deaa-4e67-81aa-916c888c2abe]
[08d6aa1c-7ec4-48e7-883a-10cd28656380]
[72153a5d-88c3-4e23-8472-64ed727d7abf]
[fea5a52b-1600-4e0e-9639-aeb3d49b0995]
[44d757f9-b27b-4d63-9038-04016f658bf7]
[0101e138-44da-4ae3-afd4-7ec37ab4614b]
[1d534153-c605-4b3c-93d4-fcfaa713aedb]
[39a997dc-3f72-4871-9a84-1630dcab3593]
[045b7757-9fdd-4f54-92ff-7d5a58327760]
[5c4b1df6-b74d-45e4-855f-3411e267cfdd]
[2c816d7f-8ff9-4658-b36f-605cf420e6be]
[03542e69-ab73-4f93-97a1-845e3a29e404]
[d5d891f0-dcd0-4cde-b31a-8ffdf2fd685e]
[ab1b5178-2008-4429-973c-d50ea02c7990]
[5dba3292-5a31-441b-8707-97cba0540c3d]
[96afc6c4-2fd7-44c8-9d3a-50264aed1934]
[762b096a-6598-42c5-a24a-eb689b72ed19]
[6270dee8-9045-4d77-8526-7439190df85b]
[2274ee74-34d2-434a-b301-dbb4b1aaf157]
[a1fd1905-12f6-45f2-9200-cd65e54532ea]
     */


    // 获取日志数据
    val logsTable = "t_hbaseSink"
    val logsRDD = getLogsRDD(logsTable, sc)
    val logsDS = spark.createDataset(logsRDD).groupBy("userString", "itemString").agg(sum("value")).drop("value").
      withColumnRenamed("sum(value)", "value")
    logsDS.select("userString").dropDuplicates().count()


    val logsRDD2 = getLogsRDD2(logsTable, sc)
    val logsDS2 = spark.createDataset(logsRDD2)
    logsDS2.select("CREATE_BY_ID").dropDuplicates().count()
    logsDS2.select("CREATE_TIME").dropDuplicates().orderBy($"CREATE_TIME".desc).show(false)

    val t1_id = "1661350d-0bd6-42d1-9ca8-0d651b5e6b96"
    logsDS2.filter($"CREATE_BY_ID" === t1_id).count()



    //read log files
    val logsRDD3 = sc.textFile("/app-ylzx-logs").filter(null != _)
    //    val logsRDD = sc.textFile("/personal/sunlu/ylzx_app").filter(null != _)

    val logsRDD3_2 = logsRDD3.map(_.split("\t")).filter(_.length == 11).filter(_ (4).length > 2).map(line => (LogView3(line(4), line(8), line(10))))

    //过滤REQUEST_URI列中包含search/getContentById.do的列，并提取PARAMS中的id
    val logsRDD3_3 = logsRDD3_2.filter(x => x.REQUEST_URI.contains("getContentById.do")).
      filter(_.PARAMS.toString.length >= 10).
      map(x => {
        val userID = x.CREATE_BY_ID.toString
        val reg2 = """id=\S*,|id=\S*}""".r
        val urlString = reg2.findFirstIn(x.PARAMS.toString).toString.replace("Some(id=", "").replace(",)", "")
        Schema1(userID, urlString)
      }).filter(_.itemString.length >= 10)


    val logsDS3 = spark.createDataset(logsRDD3_3).na.drop(Array("userString"))
    logsDS3.filter($"userString" === t1_id).count()
    logsDS3.select("userString").dropDuplicates().count() // 42

    val logsDS3_2 = spark.createDataset(logsRDD3_2)
    logsDS3_2.select($"CREATE_BY_ID").dropDuplicates().count() //64
    logsDS3_2.filter($"CREATE_BY_ID" === t1_id).count()

    logsRDD3.filter(_.contains(t1_id)).count()

    sc.stop()
    spark.stop()


  }

}
