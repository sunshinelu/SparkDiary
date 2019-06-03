package com.evayInfo.Inglory.Project.YLZX_ZTB

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object T1 {

  def main(args: Array[String]): Unit = {
    val s1 =  "2019.05.23 17:42:20"
    println(s1.length) // 19

    val s2 = "2019-05-23"
    println(s2.length) // 10

    //定义时间格式
    val dateFormat_s1 = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd") // yyyy-MM-dd HH:mm:ss或者 yyyy-MM-dd
    val data_D_s1 = dateFormat_s1.parse(s1)
    val data_S_s1 = dateFormat.format(data_D_s1)
    println(data_S_s1)

    val data_D_s2 = dateFormat.parse(s2)
    val data_S_s2 = dateFormat.format(data_D_s2)
    println(data_S_s2)


//    val creatTimeL = dateFormat2.parse(creatTimeS).getTime


    //bulid environment
    val spark = SparkSession.builder.appName("DocSimi_Title").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //connect mysql database
    val url1 = "jdbc:mysql://10.20.5.49:3306/efp5-ztb"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "BigData@2018")
    //get data
    val ds1 = spark.read.jdbc(url1, "collect_ccgp", prop1)


    val col_names = Seq("id","title", "data", "website").map(col(_))
    val ds2 = ds1.select(col_names: _*)

    ds2.select("website").dropDuplicates().show(truncate = false)


  }
}
