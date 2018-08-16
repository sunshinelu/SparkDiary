package com.evayInfo.Inglory.Project.HZB

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 18/8/15.
 *
连接：172.16.10.144
端口：3316
用户名：root
密码：bigdata
数据库：jxw
所在园区表： j_hzb_szyq
	需要字段：
  		园区名称：YQName
  		园区所在城市编码：YQCity
  		化工园区利润总额(万元)：YQXNQK6
需求：按照城市分组，每组取出利润总额前3名，再按照每个城市的利润总额进行排序，将结果写入mysql

CREATE TABLE `j_hzb_szyq_profit_test1` (
  `ID` int(11) NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `CityID` varchar(50) DEFAULT NULL COMMENT '城市ID',
  `YQName` varchar(50) DEFAULT NULL COMMENT '园区名字',
  `Profit` decimal(18,2) DEFAULT NULL COMMENT '利润',
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB AUTO_INCREMENT=246 DEFAULT CHARSET=utf8 COMMENT='所在园区利润排序分组topN';
 */

object j_hzb_szyq_TopN {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  def main(args: Array[String]) {

    SetLogger

    val sparkConf = new SparkConf().setAppName(s"j_hzb_szyq_TopN").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //connect mysql database
    val url1 = "jdbc:mysql://localhost:3306/jxw"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")
    //get data
    val ds1 = spark.read.jdbc(url1, "j_hzb_szyq", prop1).select("YQName","YQCity","YQXNQK6").na.drop()

    // groupBy YQCity, orderBy YQXNQK6, and take Top N for each group
    val w = Window.partitionBy($"YQCity").orderBy($"YQXNQK6".desc)
    val ds2 = ds1.withColumn("rn", row_number.over(w)).where($"rn" <= 3) //.drop("rn")
    ds2.show()

    // groupBy YQCity, sum YQXNQK6
    val ds3 = ds1.groupBy("YQCity").agg(sum("YQXNQK6"))
    ds3.show()

    sc.stop()
    spark.stop()

  }
}
