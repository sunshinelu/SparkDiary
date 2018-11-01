package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.recommend

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 18/11/1.
 *
 * 参考链接：
 * http://www.voidcn.com/article/p-rohynyqc-bkb.html
 *
 */
object UserBasedTest2 {

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

    SetLogger
    val SparkConf = new SparkConf().setAppName(s"UserBasedTest2").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val col_name = Seq("user","item","rating")
    val ipt_table = "recommender_test"
//    val ipt_table = "recommenderSys_Demo_Data_sample"
    val df1 = spark.read.jdbc(url,ipt_table,prop).toDF(col_name:_*)
    val train_df = df1.withColumn("user", $"user".cast("long")).
      withColumn("item", $"item".cast("long")).
      withColumn("rating", $"rating".cast("int"))

    train_df.printSchema()

    val oriRatings = train_df.rdd.map{case Row(user:Long, item:Long,rating:Int) => (user.toLong, item.toLong, rating.toInt)}

    //filter redundant (user,item,rating),this set user favorite (best-loved) 100 item
    val ratings = oriRatings.groupBy(k=>k._1).flatMap(x=>(x._2.toList.sortWith((x,y)=>x._3>y._3).take(100)))

    // one user corresponding many item
    val user2manyItem = ratings.groupBy(tup=>tup._1)
    //one user corresponding number of item
    val numPrefPerUser = user2manyItem.map(grouped=>(grouped._1,grouped._2.size))
    //join ratings with user's pref num
    //ratingsWithSize now contains the following fields: (user, item, rating, numPrefs).
    val ratingsWithSize = user2manyItem.join(numPrefPerUser).
      flatMap(joined=>{
        joined._2._1.map(f=>(f._1,f._2,f._3,joined._2._2))
      })
    //(user, item, rating, numPrefs) ==>(item,(user, item, rating, numPrefs))
    val ratings2 = ratingsWithSize.keyBy(tup=>tup._2)
    //ratingPairs format (t,iterator((u1,t,pref1,numpref1),(u2,t,pref2,numpref2))) and u1<u2
    //this don't double-count and exclude self-pairs
    val ratingPairs = ratings2.join(ratings2).filter(f=>f._2._1._1<f._2._2._1)

    val tempVectorCalcs = ratingPairs.map(data=>{
      val key = (data._2._1._1,data._2._2._1)
      val stats =
        (data._2._1._3*data._2._2._3,//rating 1 * rating 2
          data._2._1._3, //rating user 1
          data._2._2._3, //rating user 2
          math.pow(data._2._1._3, 2), //square of rating user 1
          math.pow(data._2._2._3,2), //square of rating user 2
          data._2._1._4,  //num prefs of user 1
          data._2._2._4) //num prefs of user 2
      (key,stats)
    })
    val vectorCalcs = tempVectorCalcs.groupByKey().map(data=>{
      val key = data._1
      val vals = data._2
      val size = vals.size
      val dotProduct = vals.map(f=>f._1).sum
      val ratingSum = vals.map(f=>f._2).sum
      val rating2Sum = vals.map(f=>f._3).sum
      val ratingSeq = vals.map(f=>f._4).sum
      val rating2Seq = vals.map(f=>f._5).sum
      val numPref = vals.map(f=>f._6).max
      val numPref2 = vals.map(f=>f._7).max
      (key,(size,dotProduct,ratingSum,rating2Sum,ratingSeq,rating2Seq,numPref,numPref2))
    })
    //due to matrix is not symmetry(对称) , use half matrix build full matrix
    val inverseVectorCalcs = vectorCalcs.map(x=>((x._1._2,x._1._1),(x._2._1,x._2._2,x._2._4,x._2._3,x._2._6,x._2._5,x._2._8,x._2._7)))
    val vectorCalcsTotal = vectorCalcs ++ inverseVectorCalcs

    // compute similarity metrics for each movie pair,  similarities meaning user2 to user1 similarity
    val tempSimilarities =
      vectorCalcsTotal.map(fields => {
        val key = fields._1
        val (size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, numRaters, numRaters2) = fields._2
        val cosSim = cosineSimilarity(dotProduct, scala.math.sqrt(ratingNormSq), scala.math.sqrt(rating2NormSq))*
          size/(numRaters*math.log10(numRaters2+10))
        //        val corr = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
        (key._1,(key._2, cosSim))
      })
    val similarities = tempSimilarities.groupByKey().flatMap(x=>{
      x._2.map(temp=>(x._1,(temp._1,temp._2))).toList.sortWith((a,b)=>a._2._2>b._2._2).take(50)
    })
    val temp = similarities.filter(x=>x._2._2.equals(Double.PositiveInfinity))

    val similarTable = similarities.map(x=>(x._1,x._2._1,x._2._2)).toDF()
//    spark.sql("use DatabaseName")
//    similarTable.insertInto("similar_user_test",true)


    // ratings format (user,(item,raing))
    val ratingsInverse = ratings.map(rating=>(rating._1,(rating._2,rating._3)))

    //statistics format ((user,item),(sim,sim*rating)),,,, ratingsInverse.join(similarities) fromating as (user,((item,rating),(user2,similar)))
    val statistics = ratingsInverse.join(similarities).map(x=>((x._2._2._1,x._2._1._1),(x._2._2._2,x._2._1._2*x._2._2._2)))

    // predictResult fromat ((user,item),predict)
    val predictResult = statistics.reduceByKey((x,y)=>((x._1+y._1),(x._2+y._2))).map(x=>(x._1,x._2._2/x._2._1))


    val filterItem = ratings.map(x=>((x._1,x._2),Double.NaN))
    val totalScore = predictResult ++ filterItem

    val finalResult = totalScore.reduceByKey(_+_).filter(x=> !(x._2 equals(Double.NaN))).
      map(x=>(x._1._1,x._1._2,x._2)).groupBy(x=>x._1).flatMap(x=>(x._2.toList.sortWith((x,y)=>x._3>y._3).take(50)))

    val recommendTable = finalResult.toDF()
    recommendTable.show(truncate = false)
//    spark.sql("use DatabaseName")
//    recommendTable.insertInto("recommend_user_test",true)


    sc.stop()
    spark.stop()
  }
  // *************************
  // * SIMILARITY MEASURES
  // *************************

  /**
   * The correlation between two vectors A, B is
   *   cov(A, B) / (stdDev(A) * stdDev(B))
   *
   * This is equivalent to
   *   [n * dotProduct(A, B) - sum(A) * sum(B)] /
   *     sqrt{ [n * norm(A)^2 - sum(A)^2] [n * norm(B)^2 - sum(B)^2] }
   */
  def correlation(size : Double, dotProduct : Double, ratingSum : Double,
                  rating2Sum : Double, ratingNormSq : Double, rating2NormSq : Double) = {

    val numerator = size * dotProduct - ratingSum * rating2Sum
    val denominator = scala.math.sqrt(size * ratingNormSq - ratingSum * ratingSum) *
      scala.math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum)+1

    numerator / denominator
  }

  /**
   * Regularize correlation by adding virtual pseudocounts over a prior:
   *   RegularizedCorrelation = w * ActualCorrelation + (1 - w) * PriorCorrelation
   * where w = # actualPairs / (# actualPairs + # virtualPairs).
   */
  def regularizedCorrelation(size : Double, dotProduct : Double, ratingSum : Double,
                             rating2Sum : Double, ratingNormSq : Double, rating2NormSq : Double,
                             virtualCount : Double, priorCorrelation : Double) = {

    val unregularizedCorrelation = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
    val w = size / (size + virtualCount)

    w * unregularizedCorrelation + (1 - w) * priorCorrelation
  }

  /**
   * The cosine similarity between two vectors A, B is
   *   dotProduct(A, B) / (norm(A) * norm(B))
   */
  def cosineSimilarity(dotProduct : Double, ratingNorm : Double, rating2Norm : Double) = {
    dotProduct / (ratingNorm * rating2Norm)
  }

  /**
   * The Jaccard Similarity between two sets A, B is
   *   |Intersection(A, B)| / |Union(A, B)|
   */
  def jaccardSimilarity(usersInCommon : Double, totalUsers1 : Double, totalUsers2 : Double) = {
    val union = totalUsers1 + totalUsers2 - usersInCommon
    usersInCommon / union
  }

}
