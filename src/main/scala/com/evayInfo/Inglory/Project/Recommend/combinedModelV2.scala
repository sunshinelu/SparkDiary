package com.evayInfo.Inglory.Project.Recommend


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sunlu on 17/9/4.
  * 将als、item、user和content的推荐结果保存到hbase，读取模型结果进行分析
  */

object combinedModelV2 {


  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  case class RecommSchema(userID: String, id: String, rn: Double, title: String, manuallabel: String, mod: String, modellabel: String)


  def getRecommData(tableName: String, sc: SparkContext): RDD[RecommSchema] = {
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, tableName) //设置输入表名 第一个参数yeeso-test-ywk_webpage
    /*
        conf.set("hbase.zookeeper.quorum", "192.168.37.21,192.168.37.22,192.168.37.23")
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        conf.set("hbase.master", "192.168.37.22:60000", "192.168.37.23:60000")
    */
    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("userID")) //userID
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id")) //doc id
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rn")) //level
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("title")) //title
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("manuallabel")) //manuallabel
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("mod")) //time
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("modellabel")) //modellabel
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val userID = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("userID")) //userID
      val id = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("id")) //userID
      val rn = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("rn")) //rn
      val title = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("title")) //title
      val manuallabel = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("manuallabel")) //manuallabel
      val mod = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("mod")) //mod
      val modellabel = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("modellabel")) //modellabel
      (userID, id, rn, title, manuallabel, mod, modellabel)
    }
    }.filter(x => null != x._1 & null != x._2 & null != x._3 & null != x._4 & null != x._5 & null != x._6 & null != x._7).
      map(x => {
        val userID = Bytes.toString(x._1)
        val id = Bytes.toString(x._2)
        val rn = Bytes.toString(x._3).toDouble
        //        val rn2 = (-rn.toInt + 11) * weight
        val title = Bytes.toString(x._4)
        val manuallabel = Bytes.toString(x._5)
        val mod = Bytes.toString(x._6)
        val modellabel = Bytes.toString(x._7)

        RecommSchema(userID, id, rn, title, manuallabel, mod, modellabel)
      })
    hbaseRDD
  }

  def main(args: Array[String]): Unit = {
    // 不输出日志
    RecomUtil.SetLogger
    val sparkConf = new SparkConf().setAppName(s"combinedModelV2")
    //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val ylzxTable = args(0)
    val logsTable = args(1)
    val docsimiTable = args(2)
    val outputTable = args(3)
    /*
    val ylzxTable = "yilan-total_webpage"
    val logsTable = "t_hbaseSink"
    val docsimiTable = "ylzx_xgwz"
    val outputTable = "ylzx_cnxh_combined"
     */


    val tempTable = "recommender_temp"


    alsModel.getAlsResult(ylzxTable, logsTable, sc, spark)
    contentModel.getContentResult(ylzxTable, logsTable, docsimiTable, sc, spark)
    itemModel.getItemResult(ylzxTable, logsTable, sc, spark)
    userModel.getUserResult(ylzxTable, logsTable, sc, spark)

    val w_als = 0.25
    val w_content = 0.25
    val w_item = 0.25
    val w_user = 0.25

    val w_df = sc.parallelize(Seq(("als", w_als), ("content", w_content), ("item", w_item), ("user", w_user))).
      toDF("modellabel", "weight")

    val recommRDD = getRecommData(tempTable, sc)
    val recommDF = spark.createDataset(recommRDD)

    val df = recommDF.join(w_df, Seq("modellabel"), "left").
      withColumn("wValue", (-col("rn") + 11) * col("weight")).
      drop("rn").drop("weight")

    //userID: String, id: String, rn: Double, title: String, manuallabel: String, mod: String, modellabel:String
    val itemLab = df.select("id", "title", "manuallabel", "mod").dropDuplicates()
    // 根据userID和id对wValue进行求和，新增列名为rating
    val df1 = df.groupBy("userID", "id").agg(sum("wValue")).withColumnRenamed("sum(wValue)", "rating")

    // 根据id将title、manuallabel和time整合到df2中
    val df2 = df1.join(itemLab, Seq("id"), "left").drop("wValue").na.drop()

    // 根据userString进行分组，对打分进行倒序排序，获取打分前5的数据。
    val w = Window.partitionBy("userID").orderBy(col("rating").desc)
    val df3 = df2.withColumn("rn", row_number.over(w)).where($"rn" <= 10)

    // 增加系统时间列
    val df4 = df3.select("userID", "id", "rating", "rn", "title", "manuallabel", "mod").
      withColumn("systime", current_timestamp()).withColumn("systime", date_format($"systime", "yyyy-MM-dd HH:mm:ss"))

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

    df4.rdd.map(row => (row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7))).
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

}
