package com.evayInfo.Inglory.TestCode

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix

/**
 * Created by 吴子宾 on 17-5-3.
 * 协同过滤(处理对象movie,使用算法ALS:最小二乘法(实现用户推荐)
 * 余弦相似度实现商品相似度推荐
 */
object als {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[*]").setAppName("AlsTest")
    val sc = new SparkContext(conf)
    CF(sc, "data/ml-100k/u.data")
  }

  def CF(sc: SparkContext, fileName: String): Unit = {
    val movieFile = sc.textFile(fileName)
    //将u里的用户号，电影号以及评分组成RDD  RatingDatas
    val RatingDatas = movieFile.map(_.split("\t").take(3))
    val rdtale = RatingDatas.take(3)
    val rdlen = RatingDatas.count
    //转为Ratings数据RDD ing(196,242,3.0)
    val ratings = RatingDatas.map(x => Rating(x(0).toInt, x(1).toInt, x(2).toDouble))
    val RDDratingstake = ratings.take(3)
    val rtlen = ratings.count
    //获取用户评价模型,设置k因子,和迭代次数,隐藏因子lambda,获取模型
    /*
    *   rank ：对应ALS模型中的因子个数，也就是在低阶近似矩阵中的隐含特征个数。因子个
          数一般越多越好。但它也会直接影响模型训练和保存时所需的内存开销，尤其是在用户
          和物品很多的时候。因此实践中该参数常作为训练效果与系统开销之间的调节参数。通
          常，其合理取值为10到200。
        iterations ：对应运行时的迭代次数。ALS能确保每次迭代都能降低评级矩阵的重建误
           差，但一般经少数次迭代后ALS模型便已能收敛为一个比较合理的好模型。这样，大部分
           情况下都没必要迭代太多次（10次左右一般就挺好）。
       lambda ：该参数控制模型的正则化过程，从而控制模型的过拟合情况。其值越高，正则
          化越严厉。该参数的赋值与实际数据的大小、特征和稀疏程度有关。和其他的机器学习
          模型一样，正则参数应该通过用非样本的测试数据进行交叉验证来调整。
    * */
    val model = ALS.train(ratings, 50, 10, 0.01)

    //基于用户相似度推荐
    println("userNumber:" + model.userFeatures.count() + "\t" + "productNum:" + model.productFeatures.count())
    val usetake = model.userFeatures.take(5) //943*50
    val protake = model.productFeatures.take(5) //1682*50
    //指定用户及商品,输出预测值
    println(model.predict(789, 429))
    //为指定用户推荐的前N商品
    model.recommendProducts(789, 11).foreach(println(_))
    //为每个人推荐前十个商品
    model.recommendProductsForUsers(10).take(3).foreach {
      case (x, rating) => println(rating(0))
    }
    val recocoll = model.recommendProductsForUsers(5).collect() //得出每个人推荐的矩阵943*5

    val userit = model.recommendUsersForProducts(10).collect() //给每个文章提供前10个用户
    val useit = model.recommendUsers(1011, 3) //给第1011个文章推荐前3个用户
    val userFeatures = model.userFeatures.collect() //943个用户的特征943*50
    //基于商品相似度(使用余弦相似度)进行推荐,获取某个商品的特征值
    val itemFactory = model.productFeatures.lookup(789).head //{某个商品50个特征值}
    val itemVector = new DoubleMatrix(itemFactory) //[某个商品50个特征值]
    //求余弦相似度
    val sim = model.productFeatures.map {
        //某个文章与所有1682个文章的相似度
        case (id, factory) =>
          val factorVector = new DoubleMatrix(factory)
          val sim = cosineSimilarity(factorVector, itemVector)
          (id, sim)
      }
    val simcl = sim.collect
    val sortedsim = sim.top(11)(Ordering.by[(Int,Double),Double]{
      case(id,sim)=>sim
    })
    println(sortedsim.take(10).mkString("\n")) //找出与某个文章最高的10个相似度的文章和对应的相似度

    //模型评估,通过均误差
    //实际用户评估值
    val actualRatings = ratings.map {
      //U_data数据((196,242),3.0)......
      case Rating(user, item, rats) => ((user, item), rats)
    }
    val accol = actualRatings.collect
    val userItems = ratings.map {
      //U_data数据(196,242)
      case (Rating(user, item, rats)) => (user, item)
    }
    val useitemco = userItems.collect()
    //模型的用户对商品的预测值
    val predictRatings = model.predict(userItems).map {
      //U_data打分预测的数据
      case (Rating(user, item, rats)) => ((user, item), rats)
    }
    val predictRatingscol = predictRatings.collect()

    //联合获取rate值
    val rates = actualRatings.join(predictRatings).map {
      //实际的和预测的打分值
      case x => (x._2._1, x._2._2)
    }
    val ratecol = rates.collect()
    val joincol = actualRatings.join(predictRatings).collect()
    //求均方差
    val regressionMetrics = new RegressionMetrics(rates)
    //越接近0越佳
    println(regressionMetrics.meanSquaredError)

  }

  //余弦相似度计算
  def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }
}