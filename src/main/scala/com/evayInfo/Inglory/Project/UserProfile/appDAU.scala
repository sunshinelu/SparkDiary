package com.evayInfo.Inglory.Project.UserProfile

import java.text.SimpleDateFormat
import java.util.{Properties, Calendar}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._


/**
 * Created by sunlu on 17/12/4.
 * app日活跃用户量分析（DAU）
 *
 * 任务执行代码
 *
spark-submit \
--class com.evayInfo.Inglory.Project.UserProfile.appDAU \
--master yarn \
--num-executors 2 \
--executor-cores 2 \
--executor-memory 2g \
/root/lulu/Progect/Test/SparkDiary.jar

 */
object appDAU {

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

  def getLogsRDD(logsTable: String, sc: SparkContext): RDD[LogView] = {

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
      }
      /*
      .filter(x => x.REQUEST_URI.contains("getContentById.do") || x.REQUEST_URI.contains("like/add.do") ||
      x.REQUEST_URI.contains("favorite/add.do") || x.REQUEST_URI.contains("favorite/delete.do") ||
      x.REQUEST_URI.contains("addFavorite.do") || x.REQUEST_URI.contains("delFavorite.do")
    )
       */
    hbaseRDD
  }

  //获取昨天日期（long类型）
  def getYesterdayL(): Long = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    val yesterdayL = dateFormat.parse(yesterday).getTime
    yesterdayL
  }
  //获取昨天日期（string类型）
  def getYesterday(): String = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    val yesterdayL = dateFormat.parse(yesterday).getTime
    yesterday
  }

  case class DAUschema(iosVisit:Int, iosRegist:Int, androidVist:Int, androidRegist:Int,
                       appUsers:Int, appRegUser:Int, regisUserNumber:Double, appDAU:Double,
                        analysisTime:String)//, currTime:String)
  // 昨日ios手机用户访问数、昨日ios手机访问用户中的注册用户数、昨日android手机用户访问数、昨日android手机用户访问用户中的注册用户数、
  // 昨日app用户访问数、昨日app访问用户中的注册用户数、注册用户总数、app日活跃用户在注册用户中的比例、分析数据的时间//、当前时间

  def main(args: Array[String]) {

    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"appDAU").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 获取日志数据
    val logsTable = "t_hbaseSink"
    val logsRDD = getLogsRDD(logsTable, sc)
    val logsDS = spark.createDataset(logsRDD)

//    val openNumber = logsDS.select("CREATE_BY_ID").dropDuplicates().na.drop().count().toInt
//    println("打开过易览资讯的注册用户数为：" + openNumber)
//打开过易览资讯的注册用户数为：85

//    val userNumber = logsDS.filter($"REQUEST_URI".contains("getContentById.do")).
//      select("CREATE_BY_ID").dropDuplicates().na.drop().count().toInt
//    println("使用过易览资讯的注册用户数为：" + userNumber)
    // 使用过易览资讯的注册用户数为：58


    //获取昨天时间
    val yesterdayL = getYesterdayL()
    val df1 = logsDS.filter($"CREATE_TIME_L" === yesterdayL)
//    val yesterdayUsers = df1.select("CREATE_BY_ID").na.drop().dropDuplicates().count().toInt
//    println("昨天一天注册用户的访问数量为：" + yesterdayUsers)
    // 昨天一天注册用户的访问数量为：15

//    val yesterdayReadUsers = df1.filter($"REQUEST_URI".contains("getContentById.do")).
//      select("CREATE_BY_ID").dropDuplicates().na.drop().count().toInt
//    println("昨天一天注册用户的使用数量为：" + yesterdayReadUsers)
    // 昨天一天注册用户的使用数量为：11

    // 获取昨天iOS手机用户
    def getPushId(arg:String):String = {
      val reg = "pushId=\\w*".r
      val pushId = reg.findFirstIn(arg).toString.replace("Some(pushId=", "").replace(")", "").replace("})", "")
      val result = pushId match {
        case r if r.length >= 1 => r
        case _ => ""
      }
      result
    }
    val getPushIdUdf = udf((arg:String) => getPushId(arg))
    val iosDF = df1.filter($"REQUEST_URI".contains("updatePushIdByToken.do")).
      withColumn("phoneId", getPushIdUdf($"PARAMS")).filter(length($"phoneId") >= 5)
    val iosUser = iosDF.select("phoneId").na.drop().dropDuplicates().count().toInt
//    println("iOS用户的访问数为：" + iosUser)
    // iOS用户的访问数为：20

    val iosRegUser = iosDF.select("CREATE_BY_ID").na.drop().dropDuplicates().count().toInt
//    println("iOS注册用户的访问数为：" + iosRegUser)
    // iOS注册用户的访问数为：11

    // 获取昨天安卓手机访问用户
    def getIMEI(arg:String):String = {
      val reg = "IMEI=\\w*".r
      val IMEI = reg.findFirstIn(arg).toString.replace("Some(IMEI=", "").replace(")", "").replace("})", "")
      val result = IMEI match {
        case r if r.length >= 1 => r
        case _ => ""
      }
      result
    }
    val getIMEIUdf = udf((arg:String) => getIMEI(arg))
    val androidDF = df1.filter($"REQUEST_URI".contains("init") && $"PARAMS".contains("IMEI")).
      withColumn("phoneId", getIMEIUdf($"PARAMS")).filter(length($"phoneId") >= 5)
    val androidUser = androidDF.select("phoneId").na.drop().dropDuplicates().count().toInt
//    println("androidD用户的访问数为：" + androidUser)
    // androidD用户的访问数为：12

    val androidRegUser = androidDF.select("CREATE_BY_ID").na.drop().dropDuplicates().count().toInt
//    println("androidD注册用户的访问数为：" + androidRegUser)
    // androidD注册用户的访问数为：8

    val appUsers = iosUser + androidUser
//    println("昨天app用户的全部访问用户数为：" + appUsers)
    // 昨天app用户的全部访问用户数为：32

//    val appRegUser = iosRegUser + androidRegUser
//    println("昨天app用户的全部访问用户数中注册用户数为：" + appRegUser)
    // 昨天app用户的全部访问用户数中注册用户数为：19
    val appRegDF = iosDF.select("CREATE_BY_ID", "phoneId").union(androidDF.select("CREATE_BY_ID", "phoneId"))
    val appRegUserNumber = appRegDF.select("CREATE_BY_ID").dropDuplicates().count().toInt
//    appRegDF.withColumn("label",lit(1)).groupBy("CREATE_BY_ID").agg(sum("label")).show(false)


    /*
    获取注册用户数
     */
    // 表名
    val registTable = "AC_OPERATOR"

    // 链接mysql数据库
    val url1 = "jdbc:mysql://192.168.37.102:3306/ylzx?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val prop1 = new Properties()
    prop1.setProperty("driver", "com.mysql.jdbc.Driver")
    prop1.setProperty("user", "ylzx")
    prop1.setProperty("password", "ylzx")

    // 读取注册数据
    val registDF = spark.read.jdbc(url1, registTable, prop1)
    // 获取用户数量
    val regisUserNumber = registDF.select("OPERATOR_ID").distinct().count().toDouble

    // DAU
    val dau = (appUsers / regisUserNumber).formatted("%1.5f").toDouble
//    println("DAU值为：" + dau)
    // DAU值为：0.06838

    val yesterday  = getYesterday()
    val resultTable = "YLZX_USERPROFILE_appDAU"
    val dauDF = spark.createDataFrame(Seq(DAUschema(iosUser,iosRegUser,androidUser,androidRegUser,
      appUsers,appRegUserNumber,regisUserNumber,dau,yesterday)))
      .withColumn("currTime", current_timestamp()).withColumn("currTime", date_format($"currTime", "yyyy-MM-dd HH:mm:ss"))

    //将retentionDF保存到mysql数据库中
    val url2 = "jdbc:mysql://192.168.37.18:3306/recommender_test?useUnicode=true&characterEncoding=UTF-8"
    //使用"?useUnicode=true&characterEncoding=UTF-8"以防止出现存入MySQL数据库中中文乱码情况
    val prop2 = new Properties()
    prop2.setProperty("user", "root")
    prop2.setProperty("password", "root")
    dauDF.write.mode("append").jdbc(url2, resultTable, prop2)


    sc.stop()
    spark.stop()
  }

}
