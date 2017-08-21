package com.evayInfo.Inglory.Project.DocsSimilarity.Test

import com.evayInfo.Inglory.Project.DocsSimilarity.UtilTool
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by sunlu on 17/8/21.
  */
object DocsimiCountVectorizerTest {

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def main(args: Array[String]): Unit = {

    UtilTool.SetLogger

    val sparkConf = new SparkConf().setAppName(s"DocsimiCountVectorizerTest") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


  }
}
