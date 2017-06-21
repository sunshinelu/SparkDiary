package com.evayInfo.Inglory.SparkDiary.database.es

/**
 * Created by sunlu on 17/3/13.
 * 将spark中的数据，保存到elasticsearch中。
 */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.sql._
import org.apache.spark.rdd.RDD._
import org.elasticsearch.spark._



object esDemo3 {
  //定义Person　case class
  case class Person(name: String, age: Int)
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Demo4"))
    //创建sqlContext
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //创建DataFrame
    val people = sc.textFile("/Users/sunlu/Software/spark-2.0.2-bin-hadoop2.6/examples/src/main/resources/people.txt").
      map(_.split(",")).map(p => Person(p(0),p(1).trim.toInt)).toDF()

    people.saveToEs("spark/people")
  }
  }
