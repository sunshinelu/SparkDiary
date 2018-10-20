package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.association.rules

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.fpm.FPGrowthModel
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by sunlu on 18/10/19.
 * 功能：使用构建好的FPGrowth模型对输入数据进行预测
 * 输入：模型路径、输入表名、列名、字符串分隔符、confidence参数、输出表名
 * 输出：预测后的结果
 *
 * 输入：
 * model_path:模型所在路径，String类型
 * ipt_table:输入表的表名，String类型
 * col_name:输入列的列名，String类型
 * sep:分隔符，String类型
 * confidence:设置confidence参数，Double类型
 * opt_table:输出表的表名，String类型
 *
 */
class FPGrowthModelApplication {
  // 是否输出日志
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  // 链接mysql配置信息
  val url = "jdbc:mysql://localhost:3306/data_mining_DB?useUnicode=true&characterEncoding=UTF-8&" +
    "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
  val user = "root"
  val password = "root"
  val prop = new Properties()
  prop.setProperty("user", user)
  prop.setProperty("password", password)

  def FPGrowthModelApplication(model_path:String, ipt_table:String,col_name:String,sep:String,confidence:Double,opt_table:String) = {

    val SparkConf = new SparkConf().setAppName(s"FPGrowthModelApplication:FPGrowthModelApplication").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 读取mysql数据
    val ipt_df = spark.read.jdbc(url, ipt_table, prop).select(col_name)
    val ipt_rdd1 = ipt_df.rdd.map { case Row(x: String) => x }

    val fpg_model = FPGrowthModel.load(sc, model_path)
    // predict function

    val minConfidence = confidence

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

    for (i <- ipt_rdd1.collect()) {
      val temp = predic_Func_3(fpg_model, i, sep)

      if (temp.length >= 1) {
        predict_arr += i.toString + "=>" + temp.toString
      } else {
        predict_arr += i.toString + "=>" + "null"
      }
    }


    val opt_df = sc.parallelize(predict_arr).map { x =>
      val temp = x.split("=>")
      (temp(0), temp(1))
    }.toDF(col_name, "prediction")

//    opt_df.show()
    opt_df.write.mode("overwrite").jdbc(url, opt_table, prop) //overwrite ; append


    sc.stop()
    spark.stop()
  }

}
