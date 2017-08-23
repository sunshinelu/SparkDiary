package com.evayInfo.Inglory.Project.Recommend.Example1


import breeze.linalg.{DenseVector => DV}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, MatrixEntry}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sunlu on 17/8/23.
 */
object als1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    //0 构建Spark对象
    val conf = new SparkConf().setAppName("ALS").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //    Logger.getRootLogger.setLevel(Level.WARN)

    //1 读取样本数据
    val data = sc.textFile("data/ALStest.data")
    val ratings = data.map(_.split(',') match {
      case Array(user, item, rate) =>
        Rating(user.toInt, item.toInt, rate.toDouble)
    })
    ratings.collect().foreach(println)
    //2 建立模型
    val rank = 8
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    /*
        //3 预测结果
        val usersProducts = ratings.map {
          case Rating(user, product, rate) =>
            (user, product)
        }
        val predictions =
          model.predict(usersProducts).map {
            case Rating(user, product, rate) =>
              ((user, product), rate)
          }
        val ratesAndPreds = ratings.map {
          case Rating(user, product, rate) =>
            ((user, product), rate)
        }.join(predictions)
        val MSE = ratesAndPreds.map {
          case ((user, product), (r1, r2)) =>
            val err = (r1 - r2)
            err * err
        }.mean()
        println("Mean Squared Error = " + MSE)
        */

    //4 保存/加载模型
    //    model.save(sc, "myModelPath")
    //    val sameModel = MatrixFactorizationModel.load(sc, "myModelPath")

    val userFeatures = model.userFeatures
    //    println(userFeatures.lookup(4).toArray)

    val useruserFeatures2 = userFeatures.map {
      case (id: Int, vec: Array[Double]) => {
        val vector = Vectors.dense(vec)
        new IndexedRow(id, vector) //vector的长度为rank值
      }
    } //.collect().foreach(println)

    val mat = new IndexedRowMatrix(useruserFeatures2)
    val mat_t = mat.toCoordinateMatrix.transpose()
    val sim = mat_t.toRowMatrix.columnSimilarities()
    val sim2 = sim.entries.map { case MatrixEntry(i, j, u) => (i, j, u) }
    println("user-user similarity is: ")
    sim2.collect().foreach(println)
    //    余弦相似度计算
    //    def cosineSimilarity(vec1:DV,vec2:DV):Double={
    //      vec1.dot(vec2) / (vec1.norm() * vec2.norm())
    //    }

    val itemFeatures = model.productFeatures
    //    itemFeatures.collect().foreach(println)

    val itemFeatures2 = itemFeatures.map {
      case (id: Int, vec: Array[Double]) => {
        val vector = Vectors.dense(vec)
        new IndexedRow(id, vector)
      }
    } //.collect().foreach(println)
    println("item features is: ")
    itemFeatures2.collect().foreach(println)

    val mat_item = new IndexedRowMatrix(itemFeatures2)
    val mat_item_t = mat_item.toCoordinateMatrix.transpose()
    val sim_t = mat_item_t.toRowMatrix.columnSimilarities()
    val sim_t2 = sim_t.entries.map { case MatrixEntry(i, j, u) => (i, j, u) }
    println("item-item similarity is: ")
    sim_t2.collect().foreach(println)

    sc.stop()

  }
}
