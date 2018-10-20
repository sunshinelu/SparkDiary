package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.association.rules

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.fpm.{FPGrowthModel, FPGrowth}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by sunlu on 18/10/20.
 */
object FPGrowth_Test8 {


  // 是否输出日志
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {


    // 链接mysql配置信息
    val url = "jdbc:mysql://localhost:3306/data_mining_DB?useUnicode=true&characterEncoding=UTF-8&" +
      "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val user = "root"
    val password = "root"
    val prop = new Properties()
    prop.setProperty("user", user)
    prop.setProperty("password", password)


    val ipt_table = "FPGrowth_data"
    val col_name = "items"
    val sep = " "
    val support = 0.2
    val confidence = 0.8
    val partitions = 10
    val opt_table = "FPGrowth_data_pred_test8"
    val model_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/fpg_model_test8"


    val SparkConf = new SparkConf().setAppName(s"FPGrowth_Test8").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    // 读取mysql数据
    val df_mysql = spark.read.jdbc(url, ipt_table, prop)
    val rdd1_mysql = df_mysql.select(col_name).rdd.map { case Row(x: String) => x }
    val rdd2_mysql = rdd1_mysql.map(s => s.trim.split(sep))

    // 建模参数设置
    val minSupport = support // 0.2
    val minConfidence = confidence // 0.8
    val numPartitions = partitions // 10

    // 构建FPGrowth模型
    val fpg = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartitions)

    // using rdd2_mysql build fpg_model_mysql model
    val fpg_model_mysql = fpg.run(rdd2_mysql)
    // save fpg_model_spark model
    //    fpg_model_mysql.save(sc, model_path)

    // load fpg_model
    val fpg_model_mysql_load = FPGrowthModel.load(sc, model_path)

    def predic_Func_3(fpg_model: FPGrowthModel[_], ipt: String, sep: String): String = {
      val items = ipt.trim.split(sep).toSet

      val proposals = fpg_model.generateAssociationRules(minConfidence).map { rule =>
        val antecedent = rule.antecedent.toSeq.asInstanceOf[Seq[String]]
        val consequent = rule.consequent.toSeq.asInstanceOf[Seq[String]]
        if (items != null) {
          //          val itemset = items//.toSet
          antecedent.flatMap(ant =>
            if (items != null && ant.forall(item => items.contains(item.toString))) {
              consequent.filter(item => !items.contains(item.toString))
            } else {
              Seq.empty[String]
            }) //.distinct
        } else {
          Seq.empty[String]
        }
      }.reduce(_ ++ _).distinct
      val result = proposals.mkString(sep)
      if (result.length >= 1) {
        result
      } else {
        "null"
      }
    }

    //    val predict_arr = new ArrayBuffer[Seq[(String, String)]]()
    val predict_arr = new ArrayBuffer[String]()

    for (i <- rdd1_mysql.collect()) {
      println("input: " + i)
      val t1 = predic_Func_3(fpg_model_mysql_load, i, sep)
      println("output: " + t1)
      if (t1.length >= 1) {
        predict_arr += i.toString + "=>" + t1.toString
      } else {
        predict_arr += i.toString + "=>" + "null"
      }


    }

    predict_arr.foreach(println)

    val pred_df = sc.parallelize(predict_arr).map { x =>
      val temp = x.split("=>")
      (temp(0), temp(1))
    }.toDF(col_name, "prediction")
    pred_df.show(truncate = false)



    // define udf function
    val pred_mysql_udf3 = udf((col: String) => predic_Func_3(fpg_model_mysql_load, col, sep))

    // using predic_Func_1 predict rules
    val df_mysql_pred3 = df_mysql.withColumn("prediction", pred_mysql_udf3(col(col_name)))
    println("========= df_mysql_pred3 show =============")
    df_mysql_pred3.show(truncate = false)

    /*
    报错：
     org.apache.spark.SparkException: Failed to execute user defined function(anonfun$6: (string) => string)


     */

    sc.stop()
    spark.stop()
  }
}
