package com.evayInfo.Inglory.TestCode

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON


/**
 * Created by sunlu on 17/7/24.
 * https://www.iteblog.com/archives/1246.html
 * https://stackoverflow.com/questions/39619782/how-to-read-in-memory-json-string-into-spark-dataframe?noredirect=1&lq=1
 */
object jsonToJson {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    val conf = new SparkConf().setAppName(s"jsonToJson").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext


    //    val json:JSONArray = JSONArray

    /*
    参考链接：
    https://stackoverflow.com/questions/39619782/how-to-read-in-memory-json-string-into-spark-dataframe?noredirect=1&lq=1
     */
    val otherPeopleRDD = sc.makeRDD(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val otherPeople = spark.read.json(otherPeopleRDD)
    otherPeople.show()


    /*
    将data frame转成List[String]
     */
    println("将data frame转成List[String]测试")
    val jsonDF = spark.read.json("data/people.json")
    jsonDF.show()
    val jsonString = jsonDF.toJSON //.rdd.map{case (jsonS: String) => (jsonS)}
    jsonString.collect().foreach(println)
    val jsonList = jsonString.collect().toList
    println("jsonString is: " + jsonList)

    val rdd2 = sc.parallelize(jsonList)
    val df2 = spark.read.json(rdd2)
    df2.show()
    println("将data frame转成List[String]测试OVER")

    val str2 = "{\"et\":\"kanqiu_client_join\",\"vtm\":1435898329434,\"body\":{\"client\":\"866963024862254\",\"client_type\":\"android\",\"room\":\"NBA_HOME\",\"gid\":\"\",\"type\":\"\",\"roomid\":\"\"},\"time\":1435898329}"
    val b = JSON.parseFull(str2)
    b match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(map: Map[String, Any]) => println(map)
      case None => println("Parsing failed")
      case other => println("Unknown data structure: " + other)
    }



    val jsonStr = """{ "metadata": { "key": 84896, "value": 54 }}"""
    val rdd = sc.parallelize(Seq(jsonStr))
    val df = spark.read.json(rdd)
    df.show()

    println("==========")
    val s1 =
      """{"name":"Michael"}
        |{"name":"Andy", "age":30}
        |{"name":"Justin", "age":19}"""
    val s2 = sc.textFile("data/people.json")
    s2.collect().foreach(println)
    val rdd1 = sc.makeRDD(s1 :: Nil)
    val df1 = spark.read.json(s2)
    df1.show()

  }

  /*
  JsonToJsonFunc: json list to DF to json list

   */

  def JsonToJsonFunc(spark: SparkSession, sc: SparkContext, input: List[String]): List[String] = {
    // convert json List[String] to RDD
    val jsonRdd = sc.parallelize(input)
    // convert RDD to dataframe
    val jsonDF = spark.read.json(jsonRdd)
    // convert dataframe to List[String]
    val output = jsonDF.toJSON.collect().toList
    output

  }

}
