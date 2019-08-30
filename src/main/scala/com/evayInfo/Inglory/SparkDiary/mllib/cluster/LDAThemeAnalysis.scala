package com.evayInfo.Inglory.SparkDiary.mllib.cluster

/**
  * @Author: sunlu
  * @Date: 2019-08-30 08:52
  * @Version 1.0
  *
  * 参考链接：
  * 主题分析模型LDA的spark实现
  * https://blog.csdn.net/ZH519080/article/details/85010327
  *
  * https://blog.csdn.net/u012879957/article/details/81051835
  */

import org.apache.spark.ml.clustering.{LDA, LDAModel}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, HashingTF, Tokenizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}


class LDAThemeAnalysis {

  private def data(sparkContext: SparkContext): DataFrame ={

    val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._
    val data = sparkContext.parallelize(Seq(
      (1,Array("祖国","万岁")),
      (2,Array("中华人民共和国","雄起")),
      (3,Array("万岁","中国")),
      (4,Array("祖国","雄起")),
      (5,Array("中华","雄起")),
      (6,Array("雄起")))).map{x =>
      (x._1,x._2)
    }.toDF("id","context")
    data
  }

  private def valueCompute(dataFrame: DataFrame): Unit ={

    val cv = new CountVectorizer()
      .setInputCol("context").setOutputCol("features")
    val cvmodel: CountVectorizerModel = cv.fit(dataFrame)
    val cvResult: DataFrame = cvmodel.transform(dataFrame)
    /**获得转成向量时词表*/
    val vocabulary = cvmodel.vocabulary

    /**setK:主题（聚类中心）个数
      * setMaxIter：最大迭代次数
      * setOptimizer:优化计算方法，支持”em“和”online“
      * setDocConcentration：文档-主题分布的堆成先验Dirichlet参数，值越大，分布越平滑，值>1.0
      * setTopicConcentration:文档-词语分布的先验Dirichlet参数，值越大，分布越平滑，值>1.0
      *setCheckpointInterval:checkpoint的检查间隔
      * */
    val lda = new LDA()
      .setK(3)
      .setMaxIter(20)
      .setOptimizer("em")
      .setDocConcentration(2.2)
      .setTopicConcentration(1.5)

    val ldamodel: LDAModel = lda.fit(cvResult)
    /**可能度*/
    ldamodel.logLikelihood(cvResult)
    /**困惑度，困惑度越小，模型训练越好*/
    ldamodel.logPerplexity(cvResult)

    val ladmodel: DataFrame = ldamodel.transform(cvResult)
    ladmodel.collect().foreach(println)
    /*
[1,WrappedArray(祖国, 万岁),(6,[1,2],[1.0,1.0]),[0.33209340462307646,0.33839388817170946,0.3295127072052139]]
[2,WrappedArray(中华人民共和国, 雄起),(6,[0,5],[1.0,1.0]),[0.3342385730905876,0.32938678974773616,0.33637463716167615]]
[3,WrappedArray(万岁, 中国),(6,[1,3],[1.0,1.0]),[0.33161984853221005,0.34072576820889855,0.3276543832588913]]
[4,WrappedArray(祖国, 雄起),(6,[0,2],[1.0,1.0]),[0.3333500792444844,0.33295626454838495,0.33369365620713054]]
[5,WrappedArray(中华, 雄起),(6,[0,4],[1.0,1.0]),[0.33420911525617114,0.32936928540572835,0.33642159933810045]]
[6,WrappedArray(雄起),(6,[0],[1.0]),[0.3337470127993637,0.3314444177755309,0.33480856942510545]]
     */

  }

}

object LDAThemeAnalysis {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("lda theme analysis").setMaster("local[*]")
    val sparkContext = SparkContext.getOrCreate(sparkConf)

    val lDAThemeAnalysis = new LDAThemeAnalysis
    val dataFrame = lDAThemeAnalysis.data(sparkContext)
    lDAThemeAnalysis.valueCompute(dataFrame)
    println("。。。。。。。。。 我很高兴啊 。。。。。。。。。")
  }
}
