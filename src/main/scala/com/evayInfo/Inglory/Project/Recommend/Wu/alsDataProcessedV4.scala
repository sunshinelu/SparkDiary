package com.evayInfo.Inglory.Project.Recommend.Wu

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

/**
 *
 * -Dspark.master=local
 */
object alsDataProcessedV4 {

  case class Schema(itemString: String, title: String, manuallabel: String, time: String, websitename: String)

  case class LogView(CREATE_BY_ID: String, REQUEST_URI: String, PARAMS: String)

  case class Schema1(userString: String, itemString: String)

  case class Schema2(userID: Long, urlID: Long, rating: Double)

  case class sch1(userID: Int, urlID: Int, score: Double)

  def main(args: Array[String]) {
    def convertScanToString(scan: Scan) = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") //yyyy-MM-dd
    //获取开始时间
    val startData = new Date()
    var startTime = startData.getTime.toDouble
    val time1 = dateFormat.format(startTime)
    //bulid environment
    val spark = SparkSession.builder.appName("recommendSystem").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    val tableName = "yilan-total_webpage" //  "t_TDT_result"  // t_hbaseSink总t_logs_sun表
    //定义时间格式
    // val dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss z", Locale.ENGLISH)

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围

    conf.set(TableInputFormat.INPUT_TABLE, tableName) //设置输入表名 第一个参数yeeso-test-ywk_webpage
    //conf.set(TableInputFormat.INPUT_TABLE, "yilan-ywk_webpage")
    //    readAndWrite.read(conf,sc,tableName)
    //如果outputTable表存在，则删除表；如果不存在则新建表。
    val outputTable = "t_wu_test"
    val hAdmin = new HBaseAdmin(conf)
    if (hAdmin.tableExists(outputTable)) {
      hAdmin.disableTable(outputTable)
      hAdmin.deleteTable(outputTable)
    }
    val htd = new HTableDescriptor(outputTable)
    htd.addFamily(new HColumnDescriptor("info".getBytes()))
    hAdmin.createTable(htd)


    //指定输出格式和输出表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, outputTable) //设置输出表名，与输入是同一个表t_userProfileV1
    //创建job
    val job = new Job(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("t")) //title
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //label
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("mod")) //time
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("websitename")) //

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
      val webName = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("websitename")) //时间列
      (urlID, title, manuallabel, time, webName)
    }
    }.filter(x => null != x._2 & null != x._3 & null != x._5).
      map { x => {
        val urlID_1 = Bytes.toString(x._1)
        val title_1 = if (null != x._2) Bytes.toString(x._2) else ""
        val manuallabel_1 = if (null != x._3) Bytes.toString(x._3) else ""
        //时间格式转化
        val time_1 = if (null != x._4) {
          val time = Bytes.toLong(x._4)
          val date: Date = new Date(time)
          val temp = dateFormat.format(date)
          temp
        } else ""

        val websitename_1 = if (null != x._5) Bytes.toString(x._5) else ""
        Schema(urlID_1, title_1, manuallabel_1, time_1, websitename_1)
      }
      }.filter(x => {
      x.title.length >= 2
    })

    //    val hbaseDF = spark.createDataset(hbaseRDD).as[Schema]
    val hbaseDF = hbaseRDD.toDS().as[Schema]
    hbaseDF.persist()
    //    val hbasDF = hbaseDF.take(3)       //Schema(00012fc0-b750-407e-8b31-dadcf132ecb4,习近平侨务论述研讨会在京举行 侨务工作要有大格局,政务,2016-10-28,山东省人民政府侨务办公室)
    //    val hbasDFcount = hbaseDF.count()  //114586

    //read log files
    val logsRDD = sc.textFile("hdfs:///wuzb").filter(null != _) //app-ylzx-logs
    logsRDD.persist()
    //    val logsRDDtake = logsRDD.take(3)     //88ca0a63-ebef-40d3-9b71-eeb5140cfd4a	1		 	 	2017-08-06 00:17:29	216.244.66.249	Mozilla/5.0 (compatible; DotBot/1.1; http://www.opensiteexplorer.org/dotbot, help@moz.com)	/search/tag/%E7%A7%91%E6%8A%80.do	GET	{}
    //    val logsRDD = sc.textFile("/personal/sunlu/ylzx_app").filter(null != _)
    val logsRDD02 = logsRDD.map(_.split("\t")).filter(_.length == 11)
    //    val logrdd02take = logsRDD02.take(3)
    val logsRDD2 = logsRDD02.filter(_ (4).length > 2).map(line => (LogView(line(4), line(8), line(10))))
    //得出CREATE_BY_ID,REQUEST_URI和PARAM
    //    val logrdd2count = logsRDD2.count()
    //    val logsRDD2take = logsRDD2.take(3)      //LogView(be446473-b69e-4ff9-be31-d804d77b15ac,/lmgl/queryLmList.do,{})
    //过滤REQUEST_URI列中包含search/getContentById.do的列，并提取PARAMS中的文章id
    val logsrddsearch = logsRDD2.filter(x => x.REQUEST_URI.contains("search/getContentById.do"))
    //    val logsrddsearchtake = logsrddsearch.take(2)  //LogView(be446473-b69e-4ff9-be31-d804d77b15ac,/search/getContentById.do,{id=8182d8ad-9be5-4ea6-a351-daa599a3c643, dqwz=本地})

    val logsRDD3 = logsrddsearch.filter(_.PARAMS.toString.length >= 10).
      map(x => {
        val userID = x.CREATE_BY_ID.toString
        val reg2 = """id=\S*,|id=\S*}""".r //正则化
        val urlString = reg2.findFirstIn(x.PARAMS.toString).toString.replace("Some(id=", "").replace(",)", "")
        Schema1(userID, urlString)
      }).filter(_.itemString.length >= 10)
    //    val logsRDD3take = logsRDD3.take(3)       //Schema1(be446473-b69e-4ff9-be31-d804d77b15ac,8182d8ad-9be5-4ea6-a351-daa599a3c643)
    //    val logsrdd3count = logsRDD3.count

    val logsDS = spark.createDataset(logsRDD3).na.drop(Array("userString"))
    //    val logsDStake = logsDS.take(3)   //[be446473-b69e-4ff9-be31-d804d77b15ac,8182d8ad-9be5-4ea6-a351-daa599a3c643]
    //search用户ID和文章ID计数，value
    val ds1 = logsDS.groupBy("userString", "itemString").agg(count("userString")).withColumnRenamed("count(userString)", "value")
    //    val ds1col = ds1.count
    //    val ds1take = ds1.take(3)  //[2b03592d-96a7-4812-9de2-d80ab690231e,f52bd6df-0dc8-424b-a3f1-a65400f7d465,1]
    //string to number
    val userID = new StringIndexer().setInputCol("userString").setOutputCol("userID").fit(ds1) //hdfs://cluster2/wuzb/log_2017-08-06.log:0+476529

    val ds2 = userID.transform(ds1)
    //    val ds2take = ds2.take(3)         //[2b03592d-96a7-4812-9de2-d80ab690231e,f52bd6df-0dc8-424b-a3f1-a65400f7d465,1,4.0] 用户 文章 value userID
    val urlID = new StringIndexer().setInputCol("itemString").setOutputCol("urlID").fit(ds2) //hdfs://cluster2/wuzb/log_2017-08-06.log:0+476529
    val ds3 = urlID.transform(ds2)
    //    val ds3take = ds3.take(3)    //[2b03592d-96a7-4812-9de2-d80ab690231e,f52bd6df-0dc8-424b-a3f1-a65400f7d465,1,4.0,0.0]  +文章ID
    val ds4 = ds3.withColumn("userID", ds3("userID").cast("long")).
      withColumn("urlID", ds3("urlID").cast("long")).
      withColumn("value", ds3("value").cast("double"))
    //    val ds4take = ds4.take(29)    //[2b03592d-96a7-4812-9de2-d80ab690231e,f52bd6df-0dc8-424b-a3f1-a65400f7d465,1.0,4,0]  将用户和文章ID变成Long 将value换成Double
    //    val ds4lo = ds4.count()

    //+++++++++++++++++++++++++

    //Min-Max Normalization[-1,1]
    val minMax = ds4.agg(max("value"), min("value")).withColumnRenamed("max(value)", "max").withColumnRenamed("min(value)", "min") //取出value值得最大最小值[max: double, min: double]
    //    val minmaxtake = minMax.first() //[41.0,1.0]
    val maxValue = minMax.select("max").rdd.map { case Row(d: Double) => d }.first //[43fbb93d-5a67-4a73-8173-158f8bb0d052,f52bd6df-0dc8-424b-a3f1-a65400f7d465,41.0,2,0]

    val minValue = minMax.select("min").rdd.map { case Row(d: Double) => d }.first
    //limit the values to 4 digit
    val ds5 = ds4.withColumn("norm", bround((((ds4("value") - minValue) / (maxValue - minValue)) * 2 - 1), 4)) //将value换成正则化norm
    //    val ds5take = ds5.take(29)    //[2b03592d-96a7-4812-9de2-d80ab690231e,f52bd6df-0dc8-424b-a3f1-a65400f7d465,1.0,4,0,-1.0]
    //RDD to RowRDD
    val alsRDD = ds5.select("userID", "urlID", "norm").rdd.map { row => (row(0), row(1), row(2)) }    //将userID,文章ID,分数拿出来
      .map { x =>
        val user = x._1.toString.toInt
        val item = x._2.toString.toInt
        val rate = x._3.toString.toDouble
        Rating(user, item, rate)
    }

    //    val alstake = alsRDD.take(3)      //Rating(4,0,-1.0)
    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10
    val model = ALS.train(alsRDD, rank, numIterations, 0.01)

    /////////////

    logsRDD.unpersist()
    /////////////


    val topProducts = model.recommendProductsForUsers(15)
    topProducts.persist
    //    val toptale = topProducts.take(3)       //Rating(0,4,0.23179621323076122)
    //    val topcount = topProducts.count
    val topProductsRowRDD = topProducts.flatMap(x => {
      val y = x._2
      for (w <- y) yield (w.user, w.product, w.rating)
    }).map { f => Schema2(f._1.toLong, f._2.toLong, f._3) } //取出用户，文章，评分
    //    val topRowtake = topProductsRowRDD.take(3)    //Schema2(0,4,0.23179621323076122)　
    //    val topProwcount = topProductsRowRDD.count
    val topProductsDF = spark.createDataset(topProductsRowRDD)
    //    val topDFtake = topProductsDF.take(3)    //Schema2(0,5,0.10284887526117603)
    val userLab = ds5.select("userString", "userID").dropDuplicates
    val itemLab = ds5.select("itemString", "urlID").dropDuplicates

    val joinDF1 = topProductsDF.join(userLab, Seq("userID"), "left")
    //    topProductsDF.persist()                //用户ID，文章ID，用户
    //    val joindf1take = joinDF1.take(3)     //[0,5,0.3637110008548584,cbb69e75-f2d4-4e02-b6b0-0e89f4d237bd]  +userID
    val joinDF2 = joinDF1.join(itemLab, Seq("urlID"), "left") //用户ID，文章ID，分数，用户，文章
    //    val joindf2take = joinDF2.take(3)     //[26,0,0.014542133774744767,cbb69e75-f2d4-4e02-b6b0-0e89f4d237bd,5c3ad25b-4764-4eb4-9d7e-8680fcc5b19c]  +文章ID
    val joinDF3 = joinDF2.join(hbaseDF, Seq("itemString"), "left") //文章，用户ID，文章ID，分数，用户，文章标题，标签，时间，网址
    //    val joindf3take = joinDF3.take(3)     //[cb18e6c5-19d8-47de-83a0-05385dc67a4e,12,1,0.014513503616505746,f4d21395-f9a7-4a40-8e63-aa04c2f73a5d,     +
    // 王毅：打造更高水平的中国－东盟战略伙伴关系,人才,2017-08-06,新华网]
    hbaseDF.unpersist()

    ////////////
    val joinDF6 = reCompuRating.reCoRating(ds4, model, spark, joinDF3)

    //获取结束时间
    val endData = new Date()
    val endTime = endData.getTime.toDouble
    val time = (endTime - startTime) / 60000.0

    joinDF6.rdd.map(row => (row(0), row(1), row(2), row(3), row(4), row(5), row(6))).
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
        (userString, itemString, rating2, rn, title, manuallabel, time)
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

        (new ImmutableBytesWritable, put)
      }
      }.saveAsNewAPIHadoopDataset(job.getConfiguration)

    sc.stop()

  }
}
