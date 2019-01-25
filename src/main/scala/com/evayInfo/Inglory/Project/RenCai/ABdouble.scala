package com.evayInfo.Inglory.Project.RenCai

import java.util.Properties

import com.evayInfo.Inglory.Project.RenCai.ImpactAnalysis.SetLogger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/*

将relation表中的A-B变成A-B和B-A

创建 relation_new 表

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for relation_new
-- ----------------------------
DROP TABLE IF EXISTS `relation_new`;
CREATE TABLE `relation_new` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '序号',
  `source_id` varchar(255) NOT NULL COMMENT '人才ID',
  `source_name` varchar(50) NOT NULL COMMENT '人才名称',
  `target_id` varchar(36) NOT NULL COMMENT '相关联人才ID',
  `target_name` varchar(50) NOT NULL COMMENT '相关联人才名称',
  `relation` varchar(50) NOT NULL COMMENT '人才关系:同学，校友，同事，下属，领导，熟悉程度，领域相关',
  `relation_object` varchar(255) DEFAULT NULL COMMENT '关系对象，即学校或单位',
  `weight` int(3) NOT NULL COMMENT '权重 0~100',
  `create_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


 */
object ABdouble {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {

    //不打印日志信息
    SetLogger

    val conf = new SparkConf().setAppName(s"ABdouble").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //connect mysql database
    val url1 = "jdbc:mysql://localhost:3306/sunluMySQL?useUnicode=true&characterEncoding=UTF-8"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")

    //get data
    val ds1 = spark.read.jdbc(url1, "relation", prop1)
    val col_name = Seq("source_id", "source_name", "target_id", "target_name", "relation", "relation_object", "weight", "create_time", "update_time")

    val ds2 = ds1.drop("id")

    val ds3 = ds2.select("target_id", "target_name","source_id", "source_name","relation", "relation_object", "weight", "create_time", "update_time").
      toDF(col_name:_*)

    val ds4 = ds2.union(ds3)

    //将结果保存到数据框中
    ds4.coalesce(10).write.mode("append").jdbc(url1, "relation_new", prop1) //overwrite



    sc.stop()
    spark.stop()
  }
}
