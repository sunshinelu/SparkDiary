package com.evayInfo.Inglory.Project.Recommend.Wu

import com.evayInfo.Inglory.Project.Recommend.Wu.alsDataProcessedV4.sch1
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.jblas.DoubleMatrix

object reCompuRating {

  def reCoRating(ds4: DataFrame, model: MatrixFactorizationModel, spark: SparkSession, joinDF3: DataFrame): DataFrame = {
    val userArtiValue = ds4.select("userID", "urlID", "value")
    import spark.implicits._
    val sc = spark.sparkContext
    //所有用户
    val allUser = ds4.select("userID").collect.distinct
    //所有文章
    val allarti = ds4.select("urlID").collect.distinct
    //用户，文章，得分
    val userReArti = Array.ofDim[String](allarti.length * allUser.length,3)
    //根据用户评分打分
    for (user <- 0 to allUser.length - 1) {
      //用户已经读的文章
      val haveRead = userArtiValue.filter($"userID" === allUser(user)(0)).select("urlID").collect.distinct
      //所有文章
      for (read <- 0 to allarti.length - 1) {
        //基于商品相似度(使用余弦相似度)进行推荐,获取某个商品的特征值
        val itemFactory = model.productFeatures.lookup(allarti(read)(0).toString.toInt).head //{某个商品50个特征值}
        val itemVector = new DoubleMatrix(itemFactory) //[某个商品50个特征值]

        //求余弦相似度
        val sim = model.productFeatures.map {
          //某个文章与所有1682个文章的相似度
          case (id, factory) =>
            val factorVector = new DoubleMatrix(factory)
            val sim = factorVector.dot(itemVector) / (factorVector.norm2() * itemVector.norm2())
            (id, sim)
        } //.toDS
        var sum = 0.0
        //        val simcl = sim.collect
        //根据读过的文章，给文章打分
        for (all <- 0 to haveRead.length - 1) {

          //读过文章的分数
          val rating = userArtiValue.filter($"userID" === allUser(user)(0)).filter($"urlID" === haveRead(all)(0)).select("value").first
          //读过文章与未读文章的相似度
          val simValue = sim.filter(x => x._1.equals(haveRead(all)(0).toString.toInt)).map { x => x._2 }.first
          sum = sum + rating(0).toString.toDouble * simValue
        }
        //得出（用户，文章，分数）
        userReArti(user * allarti.length + read)(0) = allUser(user)(0).toString
        userReArti(user * allarti.length + read)(1) = allarti(read)(0).toString
        userReArti(user * allarti.length + read)(2) = sum.toString
      }
    }

    val list = userReArti.toList
    val userReArtiRDD = sc.parallelize(list, 2).map { x => {
      val userID = x(0).toInt
      val urlID = x(1).toInt
      val score = x(2).toDouble
      sch1(userID, urlID, score)
    }
    }.toDS().as[sch1]

    userReArtiRDD.persist
    //归一化
    val minMax1 = userReArtiRDD.agg(max("score"), min("score")).withColumnRenamed("max(score)", "max").withColumnRenamed("min(score)", "min") //取出value值得最大最小值[max: double, min: double]
    //    val minmaxtake = minMax1.first()      //[41.0,1.0]
    val maxValue1 = minMax1.select("max").rdd.map { case Row(d: Double) => d }.first //
    val minValue1 = minMax1.select("min").rdd.map { case Row(d: Double) => d }.first
    val selectDF1 = userReArtiRDD.withColumn("value", bround((((userReArtiRDD("score") - minValue1) / (maxValue1 - minValue1)) * 2 - 1), 4)).select("userID", "urlID", "value") //将value换成正则化norm

    val joinScore = selectDF1.join(joinDF3, Seq("userID", "urlID"), "left")
    //    val joinScoretake = joinScore.collect
    val selectDF = joinScore.filter(x => x(4) != null && x(6) != null).withColumn("newValue", joinScore("rating") + joinScore("value"))
    val selectDFtake = selectDF.collect
    ////////////

    val w = Window.partitionBy("userString").orderBy(col("newValue").desc)
    val joinDF4 = selectDF.withColumn("rn", row_number.over(w)).where($"rn" <= 10) //文章，用户ID，文章ID，分数，用户，文章标题，标签，时间，网址，排序
    //    val joinDf4take = joinDF4.take(30)     //[f52bd6df-0dc8-424b-a3f1-a65400f7d465,0,1,0.10906443550785316,f4d21395-f9a7-4a40-8e63-aa04c2f73a5d,
    // 安徽管局召开2017年校园电信业务市场经营行为规范工作会议,政务;科技,2017-08-07,中华人民共和国工业和信息化部,1]
    val joinDF5 = joinDF4.select("userString", "itemString", "newValue", "rn", "title", "manuallabel", "time")
    //    val joindf5take = joinDF5.take(30)     //[f4d21395-f9a7-4a40-8e63-aa04c2f73a5d,f52bd6df-0dc8-424b-a3f1-a65400f7d465,0.10906443550785316,1,
    // 安徽管局召开2017年校园电信业务市场经营行为规范工作会议,政务;科技,2017-08-07]

    //改列名
    val joinDF6 = joinDF5.withColumnRenamed("newValue", "rating")
    //    val a = joinDF6.collect()
    return joinDF6
  }
}

