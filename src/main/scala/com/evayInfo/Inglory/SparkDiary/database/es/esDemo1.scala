package com.evayInfo.Inglory.SparkDiary.database.es

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * Created by sunlu on 17/3/9.
  * https://www.iteblog.com/archives/1728.html
  * http://blog.csdn.net/stark_summer/article/details/49743687
  * 在spark-defaults.conf下添加
  * 添加：
  * spark.es.nodes localhost
  * spark.es.port  9200
  * 使用elasticsearch-spark-20_2.11-5.2.2.jar
  * 测试成功！
  */
object esDemo1 {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("ESDemo1").setMaster("local")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "localhost")
    /*
      val spark = SparkSession.builder.master("local").appName("esDemo1").getOrCreate()
      val sc = spark.sparkContext
        */
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.json("/Users/sunlu/Software/spark-2.0.2-bin-hadoop2.6/examples/src/main/resources/people.json")


    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

    //sc.makeRDD(Seq(numbers,airports)).saveToEs("spark/docs")

    val people = sqlContext.read.format("es").load("spark/docs")
    people.show()

  }
}
