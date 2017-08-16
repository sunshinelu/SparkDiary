package com.evayInfo.Inglory.SparkDiary.database.mysql

import com.evayInfo.Inglory.util.mysqlUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/8/16.
 * 使用partitionBy保存mysql数据
 *
 */
object writeMysqlPartition {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    val conf = new SparkConf().setAppName(s"dfPartitionDemo").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    //    val url = "jdbc:mysql://192.168.37.18:3306/bbs?useUnicode=true&characterEncoding=UTF-8"
    val url = "jdbc:mysql://localhost:3306/bbs?useUnicode=true&characterEncoding=UTF-8&" +
      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val user = "root"
    val password = "root"

    val allTable = "t_yq_all"
    val t1Table = "t_partition"
    val t2Table = "t_normal"



    val allDF = mysqlUtil.getMysqlData(spark, url, user, password, allTable)

    println(allDF.rdd.partitions.size)

    allDF.explain
    val df2 = allDF.repartition(2)
    println(df2.rdd.partitions.size)
    df2.explain()

    //    df2.drop("title").write.mode(SaveMode.Append).jdbc(url, t1Table, prop)
    /*

        // truncate Mysql Table
        mysqlUtil.truncateMysql(url, user, password, t1Table)
        mysqlUtil.truncateMysql(url, user, password, t2Table)

        val prop = new Properties()
        prop.setProperty("user", user)
        prop.setProperty("password", password)

        df2.write.mode(SaveMode.Append).jdbc(url, t1Table, prop)


        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val t1_begin = dateFormat.format(new Date())
        val t1_beginL = dateFormat.parse(t1_begin).getTime

        /*
        Exception in thread "main" org.apache.spark.sql.AnalysisException: 'jdbc' does not support partitioning;
         */
        //    allDF.write.mode(SaveMode.Append).partitionBy("SOURCE").jdbc(url, t1Table, prop)
    //    allDF.repartition(1).write.mode(SaveMode.Append).partitionBy("SOURCE").jdbc(url, t1Table, prop)


    /*
    http://www.gatorsmile.io/numpartitionsinjdbc/

    java.lang.RuntimeException: org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider does not allow create table as select.
     */

        allDF.write.format("jdbc")
          .mode(SaveMode.Append)
          .option("dbtable", t1Table)
          .option("url", url)
          .option("user", user)
          .option("password", password)
          .option("numPartitions", "1")
          .save()

        val t1_end = dateFormat.format(new Date())
        val t1_endL = dateFormat.parse(t1_end).getTime
        val t1_between: Long = (t1_endL - t1_beginL) / 1000 //转化成秒
        val t1_hour: Float = t1_between.toFloat / 3600
        println("任务运行时间为：" + t1_between + "秒")
        println("任务运行时间为：" + t1_hour + "小时")

        allDF.write.mode(SaveMode.Append).jdbc(url, t2Table, prop)

        val t2_end = dateFormat.format(new Date())
        val t2_endL = dateFormat.parse(t2_end).getTime
        val t2_between: Long = (t2_endL - t1_endL) / 1000 //转化成秒
        val t2_hour: Float = t2_between.toFloat / 3600
        println("任务运行时间为：" + t2_between + "秒")
        println("任务运行时间为：" + t2_hour + "小时")

    */
    sc.stop
    spark.stop()
  }

}
