package com.evayInfo.Inglory.SparkDiary.database.hbase


import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 17/7/12.
 * 读取yilan-total_webpage表中数据，进行如下操作：
 * 1.删除manuallabel中标签为“内蒙古师范大学”、“政府”、“通讯电子”和“师大概况”的全部数据
 * 2.将manuallabel中“科学”替换成“科技”
 */
object deleteHbaseData {

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  case class Schema(id: String, manuallabel: String)

  def getYlzxRDD(ylzxTable: String, sc: SparkContext): RDD[Schema] = {
    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage
    //conf.set(TableInputFormat.INPUT_TABLE, "yilan-ywk_webpage")
    //指定输出格式和输出表名
    //conf.set(TableOutputFormat.OUTPUT_TABLE, args(1)) //设置输出表名，与输入是同一个表t_userProfileV1

    /*
    //创建job，方法一
    val job = new Job(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
*/
    /*
    //创建job，方法二
    val jobConf = new Configuration(conf)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)
    */
    //扫描整个表
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //label

    // scan.setTimeRange(1400468400000L, 1400472000000L)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val urlID = k.get()
      val manuallabel = v.getValue(Bytes.toBytes("p"), Bytes.toBytes("manuallabel")) //标签列
      (urlID, manuallabel)
    }
    }.filter(x => null != x._2).
      map(x => {
        val urlID_1 = Bytes.toString(x._1)
        val manuallabel_1 = Bytes.toString(x._2)
        Schema(urlID_1, manuallabel_1)
      }
      ).filter(_.manuallabel.length >= 2)

    hbaseRDD

  }


  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("deleteHbaseData").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val ylzxTable = "yilan-total_webpage"
    val ylzxRDD = getYlzxRDD(ylzxTable, sc)
    val ylzxDF = spark.createDataset(ylzxRDD) //.as[(String, String)]

    /*
    1.删除manuallabel中标签为“内蒙古师范大学”的全部数据
     */
    val delList = ylzxDF.filter(col("manuallabel").contains("政府") || col("manuallabel").contains("通讯电子")).
      drop("manuallabel").
      rdd.map { case Row(r: String) => r }.collect.toList

    val conf = HBaseConfiguration.create()
    //指定输出格式和输出表名
    @transient val table = new HTable(conf, ylzxTable)

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, ylzxTable) //设置输出表名
    val connection = HConnectionManager.createConnection(conf)
    val delTable = connection.getTable(ylzxTable)

    val deleteList: util.ArrayList[Delete] = new util.ArrayList[Delete]

    for (r <- delList) {
      val key = Bytes.toBytes(r)
      val delete = new Delete(key)
      deleteList.add(delete)
      //deleteList.add(new Delete(Bytes.toBytes(r)))
    }

    delTable.delete(deleteList)
    connection.close()
    delTable.close()

    /*
    2.将manuallabel中“科学”替换成“科技”
     */
    //字符串处理
    /*
    val replaceString = udf((content: String) => {
      content.replace("科学：", "科技")
    })

    val changeDF = ylzxDF.filter(col("manuallabel").contains("科学")).
      withColumn("newLabel", replaceString(col("manuallabel"))).drop("manuallabel")
*/
    val changeDF = ylzxDF.filter(col("manuallabel").contains("科学")).
      withColumn("newLabel", regexp_replace(col("manuallabel"), "科学", "科技")).drop("manuallabel")


    val jobConf = new Configuration(conf)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)

    changeDF.rdd.map { case Row(id: String, manuallabel: String) => (id, manuallabel) }.
      map { x => {
        val key = Bytes.toBytes(x._1)
        val put = new Put(key)
        put.add(Bytes.toBytes("p"), Bytes.toBytes("manuallabel"), Bytes.toBytes(x._2)) //manuallabel

        (new ImmutableBytesWritable, put)
      }
      }.saveAsNewAPIHadoopDataset(jobConf)

    /*
    3.检查修改后结果
     */

    val ds1ColumnsName = Seq("id", "label")
    val ds1 = ylzxDF.as[(String, String)].flatMap {
      case (id, manuallabel) => manuallabel.split(";").map((id, _))
    }.toDF(ds1ColumnsName: _*)
    ds1.select("label").dropDuplicates().show()

    val addColume = udf((args: String) => 1)
    ds1.withColumn("value", addColume($"label")).groupBy("label").agg(sum($"value")).show()

    val addColume2 = udf(() => 1)
    ds1.withColumn("value", addColume2()).groupBy("label").agg(sum($"value")).show()

    ylzxDF.withColumn("value", addColume2()).groupBy("manuallabel").agg(sum($"value")).show()

  }
}
