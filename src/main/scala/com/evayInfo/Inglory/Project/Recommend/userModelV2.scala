package com.evayInfo.Inglory.Project.Recommend

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.io.Text
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sunlu on 17/9/15.
 * 使用`YLZX_NRGL_OPER_CATEGORY`表中的 `OPERATOR_ID`列和`CATEGORY_NAME`列，
 `YLZX_NRGL_MYSUB_WEBSITE_COL` 表中的`OPERATOR_ID`和`COLUMN_ID`列计算用户相似性。
 做基于用户的推荐
 */
object userModelV2 {

  case class UserSimi(userId1: Long, userId2: Long, similar: Double)

  def main(args: Array[String]) {
    RecomUtil.SetLogger

    val SparkConf = new SparkConf().setAppName(s"userModelV2") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    /*
    val ylzxTable = args(0)
    val logsTable = args(1)
    val outputTable = args(2)
*/
    val ylzxTable = "yilan-total_webpage"
    val logsTable = "t_hbaseSink"
    val outputTable = "recommender_user"

    val url1 = "jdbc:mysql://localhost:3306/ylzx?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val prop1 = new Properties()
    prop1.setProperty("driver", "com.mysql.jdbc.Driver")
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")
    //get data
    val website_df = spark.read.jdbc(url1, "YLZX_NRGL_MYSUB_WEBSITE_COL", prop1)
    val category_df = spark.read.jdbc(url1, "YLZX_NRGL_OPER_CATEGORY", prop1)

    val website_df1 = website_df.select("OPERATOR_ID", "COLUMN_ID").
      withColumnRenamed("COLUMN_ID", "userFeature").
      withColumn("value", lit(1.0)).na.drop().dropDuplicates()
    val category_df1 = category_df.select("OPERATOR_ID", "CATEGORY_NAME").
      withColumnRenamed("CATEGORY_NAME", "userFeature").
      withColumn("value", lit(1.0)).na.drop().dropDuplicates()

    val df = website_df1.union(category_df1)


    //string to number
    val userID = new StringIndexer().setInputCol("OPERATOR_ID").setOutputCol("userID").fit(df)
    val df1 = userID.transform(df)
    val urlID = new StringIndexer().setInputCol("userFeature").setOutputCol("featureID").fit(df1)
    val df2 = urlID.transform(df1)

    val df3 = df2.withColumn("userID", df2("userID").cast("long")).
      withColumn("featureID", df2("featureID").cast("long")).
      withColumn("value", df2("value").cast("double"))

    val user1IdLab = df3.select("OPERATOR_ID", "userID").dropDuplicates().
      withColumnRenamed("userID", "user1Id").
      withColumnRenamed("OPERATOR_ID", "user1")
    val user2IdLab = df3.select("OPERATOR_ID", "userID").dropDuplicates().
      withColumnRenamed("userID", "user2Id").
      withColumnRenamed("OPERATOR_ID", "user2")
    //    val featureIdLab = df3.select("userFeature","featureID").dropDuplicates()

    //RDD to RowRDD
    val rdd1 = df3.select("userID", "featureID", "value").rdd.
      map { case Row(user: Long, item: Long, value: Double) => MatrixEntry(user, item, value) }

    //calculate similarities
    val ratings = new CoordinateMatrix(rdd1).transpose()
    val userSimi = ratings.toRowMatrix.columnSimilarities(0.1)
    // user-user similarity
    val userSimiRdd = userSimi.entries.map(f => UserSimi(f.i, f.j, f.value))

    // user1, user1, similarity
    val userSimiDF = userSimiRdd.map { f => (f.userId1, f.userId2, f.similar) }.
      union(userSimiRdd.map { f => (f.userId2, f.userId1, f.similar) }).toDF("user1Id", "user2Id", "userSimi")

    val userSimiDF2 = userSimiDF.join(user1IdLab, Seq("user1Id"), "left").
      join(user2IdLab, Seq("user2Id"), "left").na.drop.select("user1", "user2", "userSimi")



    val logsRDD = getLogsRDD(logsTable, sc)

    val logsDS = spark.createDataset(logsRDD).na.drop(Array("userString")).
      groupBy("userString", "itemString").
      agg(sum("value")).
      withColumnRenamed("sum(value)", "rating").drop("value") // "userString", "itemString" ,"rating"

    /*
  基于用户的协同过滤：data frame版本
   */

    // user1, user2, userSimi, userString, itemString, rating
    val userR_1 = userSimiDF2.join(logsDS, userSimiDF2("user1") === logsDS("userString"), "left").
      withColumn("recomValue", col("userSimi") * col("rating")).
      groupBy("user2", "itemString").agg(sum($"recomValue")).drop("recomValue").
      withColumnRenamed("sum(recomValue)", "recomValue")
    // user2, itemString, recomValue

    val logsDS2 = logsDS.select("userString", "itemString").withColumnRenamed("userString", "user2").withColumn("whether", lit(1))
    val userR_2 = userR_1.join(logsDS2, Seq("user2", "itemString"), "left").filter(col("whether").isNull)

    val ylzxRDD = RecomUtil.getYlzxRDD(ylzxTable, 20, sc)
    val ylzxDF = spark.createDataset(ylzxRDD).dropDuplicates("content").drop("content")

    val userR_3 = userR_2.join(ylzxDF, Seq("itemString"), "left")

    //对dataframe进行分组排序，并取每组的前5个
    val w = Window.partitionBy("user2").orderBy(col("recomValue").desc)
    val userR_4 = userR_3.withColumn("rn", row_number.over(w)).where(col("rn") <= 5)

    val userR_5 = userR_4.select("user2", "itemString", "recomValue", "rn", "title", "manuallabel", "time")
      .withColumn("systime", current_timestamp()).withColumn("systime", date_format($"systime", "yyyy-MM-dd HH:mm:ss"))

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围

    /*
    如果outputTable表存在，则删除表；如果不存在则新建表。
     */
    val hAdmin = new HBaseAdmin(conf)
    if (hAdmin.tableExists(outputTable)) {
      hAdmin.disableTable(outputTable)
      hAdmin.deleteTable(outputTable)
    }
    //    val htd = new HTableDescriptor(outputTable)
    val htd = new HTableDescriptor(TableName.valueOf(outputTable))
    htd.addFamily(new HColumnDescriptor("info".getBytes()))
    hAdmin.createTable(htd)

    //指定输出格式和输出表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, outputTable) //设置输出表名

    val jobConf = new Configuration(conf)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)

    userR_5.rdd.map(row => (row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7))).
      map(x => {
        val userString = x._1.toString
        val itemString = x._2.toString
        //保留rating有效数字
        val rating = x._3.toString.toDouble
        val rating2 = f"$rating%1.5f".toString
        val rn = x._4.toString
        val title = if (null != x._5) x._5.toString else ""
        val manuallabel = if (null != x._6) x._6.toString else ""
        val time = if (null != x._7) x._7.toString else ""
        val sysTime = if (null != x._8) x._8.toString else ""
        (userString, itemString, rating2, rn, title, manuallabel, time, sysTime)
      }).filter(_._5.length >= 2).
      map { x => {
        val paste = x._1 + "::score=" + x._4.toString
        val key = Bytes.toBytes(paste)
        val put = new Put(key)
        put.add(Bytes.toBytes("info"), Bytes.toBytes("userID"), Bytes.toBytes(x._1.toString)) //标签的family:qualify,userID
        put.add(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(x._2.toString)) //id
        put.add(Bytes.toBytes("info"), Bytes.toBytes("rating"), Bytes.toBytes(x._3.toString)) //rating
        put.add(Bytes.toBytes("info"), Bytes.toBytes("rn"), Bytes.toBytes(x._4.toString)) //rn
        put.add(Bytes.toBytes("info"), Bytes.toBytes("title"), Bytes.toBytes(x._5.toString)) //title
        put.add(Bytes.toBytes("info"), Bytes.toBytes("manuallabel"), Bytes.toBytes(x._6.toString)) //manuallabel
        put.add(Bytes.toBytes("info"), Bytes.toBytes("mod"), Bytes.toBytes(x._7.toString)) //mod
        put.add(Bytes.toBytes("info"), Bytes.toBytes("sysTime"), Bytes.toBytes(x._8.toString)) //sysTime

        (new ImmutableBytesWritable, put)
      }
      }.saveAsNewAPIHadoopDataset(jobConf)


    sc.stop()
    spark.stop()
  }

  case class LogView(CREATE_BY_ID: String, CREATE_TIME: Long, REQUEST_URI: String, PARAMS: String)

  case class LogView2(userString: String, itemString: String, CREATE_TIME: Long, value: Double)

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def getLogsRDD(logsTable: String, sc: SparkContext): RDD[LogView2] = {

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
      }.filter(x => x.REQUEST_URI.contains("getContentById.do") || x.REQUEST_URI.contains("like/add.do") ||
      x.REQUEST_URI.contains("favorite/add.do") || x.REQUEST_URI.contains("favorite/delete.do") ||
      x.REQUEST_URI.contains("addFavorite.do") || x.REQUEST_URI.contains("delFavorite.do")
    ).
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
          case r if (r.contains("getContentById.do")) => 0.2 * value
          case r if (r.contains("like/add.do")) => 0.3 * value
          case r if (r.contains("favorite/add.do")) => 0.5 * value
          case r if (r.contains("addFavorite.do")) => 0.5 * value //0.5
          case r if (r.contains("favorite/delete.do")) => -0.5 * value
          case r if (r.contains("delFavorite.do")) => -0.5 * value //-0.5
          case _ => 0.0 * value
        }

        LogView2(userID, urlString, time, rating)
      }).filter(_.itemString.length >= 5).filter(_.userString.length >= 5).
      map(x => {
        val userString = x.userString
        val itemString = x.itemString
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

        //val rating = rValue(time, value)
        LogView2(userString, itemString, time, rating)
      })

    hbaseRDD
  }

}
