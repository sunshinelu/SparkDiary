package com.evayInfo.Inglory.Project.Recommend

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by sunlu on 17/9/4.
  * 将als、item、user和content的推荐结果保存到hbase，读取模型结果进行分析
  */

object combinedModelV2 {

  def main(args: Array[String]): Unit = {
    // 不输出日志
    RecomUtil.SetLogger
    val sparkConf = new SparkConf().setAppName(s"combinedModel")
    //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val ylzxTable = args(0)
    val logsTable = args(1)
    val docsimiTable = args(2)
    val outputTable = args(3)
    /*
    val ylzxTable = "yilan-total_webpage"
    val logsTable = "t_hbaseSink"
    val docsimiTable = "ylzx_xgwz"
    val outputTable = "ylzx_cnxh_combined"
     */

    /*
val alsTable = "recommender_als"
val contentTable = "recommender_content"
val itemTable = "recommender_user"
val userTable = "recommender_item"
val outputTable = "recommender_combined"
 */
    alsModel.getAlsResult(ylzxTable, logsTable, sc, spark)
    contentModel.getContentResult(ylzxTable, logsTable, docsimiTable, sc, spark)
    itemModel.getItemResult(ylzxTable, logsTable, sc, spark)
    userModel.getUserResult(ylzxTable, logsTable, sc, spark)



    sc.stop()
    spark.stop()
  }

}
