package com.evayInfo.Inglory.TestCode

import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.util.MyStaticValue
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/10/23.
 * 打包成assembly jar测试加载library方法是否可用
 *
spark-submit \
--class com.evayInfo.Inglory.TestCode.ansjTest2 \
--master yarn \
--deploy-mode client \
--num-executors 1 \
--executor-cores 1 \
--executor-memory 1g \
/root/lulu/Progect/Test/SparkDiary-1.0-SNAPSHOT-jar-with-dependencies.jar

 *
 */
object ansjTest2 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  def main(args: Array[String]) {


    SetLogger

    val conf = new SparkConf().setAppName(s"ansjTest2") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    //在用词典未加载前可以通过,代码方式方式来加载
    //    MyStaticValue.userLibrary = "/root/lulu/Progect/Test/userDefine.dic"//此方法可以加载词典
    MyStaticValue.userLibrary = "/root/lulu/Progect/NLP/userDic_20171023.txt" // bigdata7路径
    val s2 = "Hadoop常见问题及解决方法大数据" +
        "让中国开发者更容易地使用TensorFlow打造人工智能应用" +
        "为什么Python发展得如此之快？"
    val seg2 = ToAnalysis.parse(s2)
    println(seg2)

    println("============")
    //    MyStaticValue.userLibrary = "hdfs:///personal/sunlu/ylzx/userDefine.dic"//此方法无法加载词典
    val seg = ToAnalysis.parse(s2)
    println(seg)

  }

}
