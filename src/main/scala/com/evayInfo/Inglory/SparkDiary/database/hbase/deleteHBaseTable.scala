package com.evayInfo.Inglory.SparkDiary.database.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.io.Text

/**
  * Created by sunlu on 17/8/1.
  */
object deleteHBaseTable {
  def main(args: Array[String]): Unit = {

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, args(0)) //设置输入表名 第一个参数yeeso-test-ywk_webpage
    //conf.set(TableInputFormat.INPUT_TABLE, "ztb-test-yangch-2_webpage")


    val outputTable = args(1)

    //判断HBAE表是否存在，如果存在则删除表，然后新建表
    val hAdmin = new HBaseAdmin(conf)
    if (hAdmin.tableExists(outputTable)) {
      hAdmin.disableTable(outputTable)
      hAdmin.deleteTable(outputTable)
    }
    val htd = new HTableDescriptor(TableName.valueOf(outputTable))
    htd.addFamily(new HColumnDescriptor("p".getBytes()))
    htd.addFamily(new HColumnDescriptor("f".getBytes()))
    /*
    val hcd = new HColumnDescriptor("id")
    //add  column to table
    htd.addFamily(hcd)
    */
    hAdmin.createTable(htd)

    //指定输出格式和输出表名
    conf.set(TableOutputFormat.OUTPUT_TABLE,outputTable) //设置输出表名，与输入是同一个表t_userProfileV1
    //创建job
    /*
val job = new Job(conf)
job.setOutputKeyClass(classOf[ImmutableBytesWritable])
job.setOutputValueClass(classOf[Result])
job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
*/
    val jobConf = new Configuration(conf)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)


  }
}
