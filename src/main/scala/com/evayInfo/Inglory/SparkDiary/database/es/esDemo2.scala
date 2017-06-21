package com.evayInfo.Inglory.SparkDiary.database.es

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

/**
 * Created by sunlu on 17/3/13.
 * http://blog.csdn.net/myproudcodelife/article/details/50985057
 * 读取elasticsearch中的数据
 */
object esDemo2 {
  def main(args: Array[String]) {
    //屏蔽日志
    //Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jrtty.server").setLevel(Level.OFF)


    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("esDemo2"))
    //创建sqlContext
    val sqlContext = new SQLContext(sc)

    val people = sqlContext.esDF("spark/people")
    println(people.schema.treeString)
    people.show()

    val wangs = sqlContext.esDF("spark/people","?q=wang")
    wangs.show()
  }
}
