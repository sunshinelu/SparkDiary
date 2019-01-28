package com.evayInfo.Inglory.Project.RenCai.PerformanceTest

import java.text.SimpleDateFormat
import java.util.{Properties, UUID}

import breeze.linalg.{max, min}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.io.Text
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{max =>f_max,udf,bround,sum,lit,current_timestamp,current_date,date_format}
import org.apache.spark.sql.{Row, SparkSession}

/*

将校友关系分析的结果保存到hbase里面
 */
object XiaoYouRelationHBase {

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

  def degree(s1:String, e1:String, s2:String, e2:String):Double={
    //定义时间格式
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
    val latest_start = max(dateFormat.parse(s1).getTime,dateFormat.parse(s2).getTime)
    val earliest_end = min(dateFormat.parse(e1).getTime, dateFormat.parse(e2).getTime)
    val overlap = (earliest_end - latest_start)/(1000*3600*24) +1
    val result = if (overlap < 0){
      0.0
    }else{
      overlap / (3 * 365.0)
    }

    val degree_result = if (result > 1){
      100.0
    }else{
      100.0 * result
    }
    return degree_result
  }

  def main(args: Array[String]): Unit = {

    //不打印日志信息
    SetLogger

    val conf = new SparkConf().setAppName(s"TongXueRelation").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //connect mysql database
    val url1 = "jdbc:mysql://localhost:3306/talent"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")

    /*
val url1 = "jdbc:mysql://10.20.7.156:3306/talent?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
val prop1 = new Properties()
prop1.setProperty("user", "root")
prop1.setProperty("password", "rcDsj_56")
 */

    val to_lower_udf = udf((x: String) => x.toLowerCase())

    //get data
    val ds1 = spark.read.jdbc(url1, "education_info", prop1)
    val ds2 = ds1.select("talent_id","school_name","start_date","end_date").na.drop().
      withColumn("talent_id", to_lower_udf($"talent_id")).dropDuplicates()

    val col_temp_1 = Seq("id_1","school_name","s1","e1")
    val temp_1 = ds2.toDF(col_temp_1: _*)

    val col_temp_2 = Seq("id_2","school_name","s2","e2")
    val temp_2 = ds2.toDF(col_temp_2: _*)

    val temp_3 = temp_1.join(temp_2,Seq("school_name"),"outer").filter($"id_1" =!= $"id_2").na.drop()

    val degree_udf = udf((s1:String, e1:String, s2:String, e2:String) => degree(s1,e1,s2,e2))

    val temp_4 = temp_3.withColumn("degree",degree_udf($"s1",$"e1",$"s2",$"e2"))

    val temp_5 = temp_4.na.drop().select("school_name", "id_1", "id_2", "degree").
      groupBy("id_1", "id_2", "school_name").agg(sum("degree"))

    def maxWeight(x: Double): Double = {
      val result = if (x > 100) {
        100.0
      } else
        x
      return result
    }

    val maxWeight_udf = udf((x: Double) => maxWeight(x))

    val temp_6 = temp_5.withColumn("degree", bround(maxWeight_udf($"sum(degree)"),3)).
      drop("sum(degree)")



    val temp_7 = temp_6.select("id_2","id_1","school_name","degree").
      toDF("id_1","id_2","school_name","degree")

    val temp_8 = temp_7.union(temp_6).groupBy("id_1","id_2","school_name").
      agg(f_max("degree")).withColumnRenamed("max(degree)","degree")

    temp_8.printSchema()



    //get data
    val info_ds = spark.read.jdbc(url1, "talent_info_new", prop1).
      select("talent_id","name").
      withColumn("talent_id", to_lower_udf($"talent_id")).
      dropDuplicates()

    val info_id1 = info_ds.toDF("id_1","name_1")
    val info_id2 = info_ds.toDF("id_2","name_2")

    val join_df = temp_8.join(info_id1,Seq("id_1"),"left").join(info_id2,Seq("id_2"),"left")

    val ds3 = join_df.withColumn("relation",lit("校友")).
      withColumn("create_time", current_timestamp()).
      withColumn("create_time", date_format($"create_time", "yyyy-MM-dd HH:mm:ss")).
      withColumn("update_time",$"create_time")

    val result_col = Seq("source_id","source_name","target_id","target_name","relation","relation_object","weight","create_time","update_time")
    val result_df = ds3.select("id_1","name_1","id_2","name_2","relation","school_name","degree","create_time","update_time").
      toDF(result_col:_*).dropDuplicates().na.drop()

//    val result_df = ds4.select("target_id","target_name","source_id","source_name","relation","relation_object","weight","create_time","update_time").
//      toDF(result_col:_*).union(ds4)

    val conf_hbase = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围

    val outputTable = "relation"

    //如果outputTable存在则不做任何操作，如果HBASE表不存在则新建表
    val hadmin = new HBaseAdmin(conf_hbase)
    if (!hadmin.isTableAvailable(outputTable)) {
      print("Table Not Exists! Create Table")
      val tableDesc = new HTableDescriptor(TableName.valueOf(outputTable))
      tableDesc.addFamily(new HColumnDescriptor("info".getBytes()))
      hadmin.createTable(tableDesc)
    } else {
      print("Table  Exists!  not Create Table")
    }

    //指定输出格式和输出表名
    conf_hbase.set(TableOutputFormat.OUTPUT_TABLE, outputTable) //设置输出表名，与输入是同一个表t_userProfileV1

    val jobConf = new Configuration(conf_hbase)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)

    result_df.rdd.map{case Row(source_id:String,source_name:String,target_id:String,target_name:String,
    relation:String,relation_object:String,weight:Double,create_time:String,update_time:String) =>
      (source_id, source_name,target_id, target_name,relation,relation_object,weight,create_time,update_time)}.
      map{ x=> {
        val id = UUID.randomUUID().toString().toLowerCase()
        val key = Bytes.toBytes(id)
        val put = new Put(key)
        put.add(Bytes.toBytes("info"), Bytes.toBytes("source_id"), Bytes.toBytes(x._1)) //source_id
        put.add(Bytes.toBytes("info"), Bytes.toBytes("source_name"), Bytes.toBytes(x._2)) //source_name
        put.add(Bytes.toBytes("info"), Bytes.toBytes("target_id"), Bytes.toBytes(x._3)) //target_id
        put.add(Bytes.toBytes("info"), Bytes.toBytes("target_name"), Bytes.toBytes(x._4)) //target_name
        put.add(Bytes.toBytes("info"), Bytes.toBytes("relation"), Bytes.toBytes(x._5)) //relation
        put.add(Bytes.toBytes("info"), Bytes.toBytes("relation_object"), Bytes.toBytes(x._6)) //relation_object
        put.add(Bytes.toBytes("info"), Bytes.toBytes("weight"), Bytes.toBytes(x._7)) //weight double类型
        put.add(Bytes.toBytes("info"), Bytes.toBytes("create_time"), Bytes.toBytes(x._8)) //create_time
        put.add(Bytes.toBytes("info"), Bytes.toBytes("update_time"), Bytes.toBytes(x._9)) //update_time

        (new ImmutableBytesWritable, put)
      }
    }.saveAsNewAPIHadoopDataset(jobConf)



    sc.stop()
    spark.stop()
  }

}
