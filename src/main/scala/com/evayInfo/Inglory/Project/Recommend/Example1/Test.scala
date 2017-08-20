package com.evayInfo.Inglory.Project.Recommend.Example1


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ItemCF {
  def main(args: Array[String]) {

    //0 构建Spark对象

    val sparkConf = new SparkConf().setAppName(s"UserCF").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    Logger.getRootLogger.setLevel(Level.WARN)

    //1 读取样本数据
    val data_path = "data/sample_itemcf2.txt"
    val data = sc.textFile(data_path)
    val userdata = data.map(_.split(",")).map(f => (ItemPref(f(0), f(1), f(2).toDouble))).cache()

    //2 建立模型
    val mysimil = new ItemSimilarity()
    val simil_rdd1 = mysimil.Similarity(userdata, "cooccurrence")
    val recommd = new RecommendedItem
    val recommd_rdd1 = recommd.Recommend(simil_rdd1, userdata, 30)

    //3 打印结果
    println(s"物品相似度矩阵: ${simil_rdd1.count()}")
    simil_rdd1.collect().foreach { ItemSimi =>
      println(ItemSimi.itemid1 + ", " + ItemSimi.itemid2 + ", " + ItemSimi.similar)
    }
    /*

物品相似度矩阵: 10
2, 4, 0.3333333333333333
3, 4, 0.3333333333333333
4, 2, 0.3333333333333333
3, 2, 0.3333333333333333
1, 2, 0.6666666666666666
4, 3, 0.3333333333333333
2, 3, 0.3333333333333333
1, 3, 0.6666666666666666
2, 1, 0.6666666666666666
3, 1, 0.6666666666666666
     */



    println(s"用戶推荐列表: ${recommd_rdd1.count()}")
    recommd_rdd1.collect().foreach { UserRecomm =>
      println(UserRecomm.userid + ", " + UserRecomm.itemid + ", " + UserRecomm.pref)
    }
    /*
    用戶推荐列表: 11
    4, 3, 0.6666666666666666
    4, 1, 0.6666666666666666
    6, 2, 0.3333333333333333
    6, 3, 0.3333333333333333
    2, 4, 0.3333333333333333
    2, 2, 1.0
    5, 4, 0.6666666666666666
    3, 2, 0.6666666666666666
    3, 1, 0.6666666666666666
    1, 4, 0.3333333333333333
    1, 3, 1.0
     */

    sc.stop()
    spark.stop()
  }
}

