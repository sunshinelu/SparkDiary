package com.evayInfo.Inglory.Project.Recommend.Wu

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CheckRecommResult {

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

  def main(args: Array[String]): Unit = {

    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"CheckRecommResult").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext

//    val ylzxTable = "wu_recommend"
    val ylzxTable = "ylzx_cnxh"
    val myID = "175786f8-1e74-4d6c-94e9-366cf1649721"

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名

    //扫描整个表中指定的列和列簇
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("userID")) //cREATE_BY_ID
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id")) //cREATE_TIME
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rn")) //rEQUEST_URI
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("title")) //pARAMS
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //提取hbase数据，并对数据进行过滤
    val hbaseRDD = hBaseRDD.map { case (k, v) => {
      val rowkey = k.get()
      val userID = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("userID")) //cREATE_BY_ID
      val id = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("id")) //cREATE_TIME
      val rn = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("rn")) //rEQUEST_URI
      val title = v.getValue(Bytes.toBytes("info"), Bytes.toBytes("title")) //pARAMS
      (userID, id, rn, title)
    }
    }.filter(x => null != x._1 & null != x._2 & null != x._3 & null != x._4).
      map { x => {
        val userID = Bytes.toString(x._1)
        val id = Bytes.toString(x._2)
        val rn = Bytes.toString(x._3)
        val title = Bytes.toString(x._4)
        (userID, id, rn, title)
      }
      }.filter(x => x._1.contains(myID))

    println(hbaseRDD.count())
    hbaseRDD.collect().foreach(println)
/*
10
(175786f8-1e74-4d6c-94e9-366cf1649721,73e3b18e-4e1f-4a80-8812-1e75daa3c7e9,1,2017年上半年建材工业经济运行简况)
(175786f8-1e74-4d6c-94e9-366cf1649721,6e94ae81-e383-4076-bcf2-6381302f25fb,10,BCG报告：中国互联网偏重商业创新，但AI领域已转向技术创新)
(175786f8-1e74-4d6c-94e9-366cf1649721,4cde36f1-4212-4fe0-9fbb-0872d726dbec,2,关于2017年度陕西省国际科技合作基地认定申报指南)
(175786f8-1e74-4d6c-94e9-366cf1649721,c3df1ae5-3d7b-46f3-8b58-5256f0a22d98,3,材料界“奥斯卡”隆重启动 2017新材料最大丰收企业都有谁？)
(175786f8-1e74-4d6c-94e9-366cf1649721,cn.gov.miit.www:http/n1146290/n1146402/n1146440/c5645093/content.html,4,科技司调研广东省制造业创新中心建设工作)
(175786f8-1e74-4d6c-94e9-366cf1649721,0de705f2-6260-4b46-a329-6b4d924efbc5,5,T112017在京举行——数据驱动指数级行业升级)
(175786f8-1e74-4d6c-94e9-366cf1649721,f097eca2-870a-4135-96c2-2a2a00ee5b13,6,中国城市地产大数据实验室在深圳成立)
(175786f8-1e74-4d6c-94e9-366cf1649721,9e112868-e010-4995-bf9d-0452aaf48806,7,北京将推动政府采购更多面向云计算和大数据服务领域)
(175786f8-1e74-4d6c-94e9-366cf1649721,ae32d0fe-5130-45b8-92be-20eba6558bed,8,住房和城乡建设部督导我市黑臭水体治理工作)
(175786f8-1e74-4d6c-94e9-366cf1649721,354f50b1-0472-4b4f-9c0c-6a3041047446,9,全力做好安保维稳各项工作)
 */
    sc.stop()
    spark.stop()
  }
}
