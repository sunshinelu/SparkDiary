package com.evayInfo.Inglory.SparkDiary.database.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.io.Text

/**
 * Created by sunlu on 17/8/1.
 */
object createHBaseTabel {
  def main(args: Array[String]): Unit = {

    val inputTable = ""
    val outputTable = ""

    val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, inputTable) //设置输入表名 第一个参数yeeso-test-ywk_webpage
    //conf.set(TableInputFormat.INPUT_TABLE, "ztb-test-yangch-2_webpage")
    //指定输出格式和输出表名
    conf.set(TableOutputFormat.OUTPUT_TABLE, outputTable) //设置输出表名，与输入不是同一个表t_userProfileV1

    //如果outputTable存在则不做任何操作，如果HBASE表不存在则新建表
    val hadmin = new HBaseAdmin(conf)
    if (!hadmin.isTableAvailable(outputTable)) {
      print("Table Not Exists! Create Table")
      val tableDesc = new HTableDescriptor(TableName.valueOf(outputTable))
      tableDesc.addFamily(new HColumnDescriptor("p".getBytes()))
      tableDesc.addFamily(new HColumnDescriptor("f".getBytes()))
      hadmin.createTable(tableDesc)
    }else{
      print("Table  Exists!  not Create Table")
    }

    //创建job
    /*
    val job = new Job(conf) # Job方法已经过时
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    */
    val jobConf = new Configuration(conf)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)


  }
}
