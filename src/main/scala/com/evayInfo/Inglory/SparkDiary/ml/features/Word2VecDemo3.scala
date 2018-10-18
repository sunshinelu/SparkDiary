package com.evayInfo.Inglory.SparkDiary.ml.features

import com.evayInfo.Inglory.SparkDiary.ml.features.Word2VecDemo1.SetLogger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.SparkSession

object Word2VecDemo3 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {
    SetLogger
    val conf = new SparkConf().setAppName(s"Word2VecDeom1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext


    val train_df = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")
    val test_df = train_df.sample(false,0.5)

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(train_df)

    val word2VecResult = model.transform(train_df)

    val model_path = "D:\\Workspace\\IDEA\\GitHub\\SparkDiary\\result\\Word2VecDemo3"
    model.write.overwrite().save(model_path)
    val load_model = Word2VecModel.load(model_path)

    val word2vec_test = load_model.transform(test_df)
    word2vec_test.show()

    sc.stop()
    spark.stop()
  }
}
