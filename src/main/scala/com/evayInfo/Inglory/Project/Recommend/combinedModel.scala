package com.evayInfo.Inglory.Project.Recommend

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.io.Text
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/9/4.
 * 合并基于als、item、user和content的推荐模型，构建组合模型
 */
object combinedModel {
  def main(args: Array[String]) {
    // 不输出日志
    RecomUtil.SetLogger

    val sparkConf = new SparkConf().setAppName(s"combinedModel")
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

    val w_als = 0.25
    val w_content = 0.25
    val w_item = 0.25
    val w_user = 0.25

    val alsDS = alsModel.getAlsModel(ylzxTable, logsTable, sc, spark).drop("rating").
      withColumn("wValue", col("rn") * w_als)
    val contentDS = contentModel.getContentModel(ylzxTable, logsTable, docsimiTable, sc, spark).drop("rating").
      withColumn("wValue", col("rn") * w_content)
    val itemDS = itemModel.getItemModel(ylzxTable, logsTable, sc, spark).drop("rating").
      withColumn("wValue", col("rn") * w_item)
    val userDS = userModel.getUserModel(ylzxTable, logsTable, sc, spark).drop("rating").
      withColumn("wValue", col("rn") * w_user)

    // 将alsDS、contenDS、itemDS和userDS合并到一个dataset中
    val recommDS = alsDS.union(itemDS).union(userDS).union(contentDS)
    //("userString", "itemString", "rating", "rn", "title", "manuallabel", "time")
    val itemLab = recommDS.select("itemString", "title", "manuallabel", "time").dropDuplicates()
    // 根据userString和itemString对rn进行求和，新增列名为rating
    val df1 = recommDS.groupBy("userString", "itemString").agg(sum("wValue")).withColumnRenamed("sum(wValue)", "rating")

    // 根据itemString将title、manuallabel和time整合到df2中
    val df2 = df1.join(itemLab, Seq("itemString"), "left").drop("rn").na.drop()

    // 根据userString进行分组，对打分进行倒序排序，获取打分前5的数据。
    val w = Window.partitionBy("userString").orderBy(col("rating").desc)
    val df3 = df2.withColumn("rn", row_number.over(w)).where($"rn" <= 5)
    // 增加系统时间列
    val df4 = df3.select("userString", "itemString", "rating", "rn", "title", "manuallabel", "time").
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
        val rating = x._3.toString
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
