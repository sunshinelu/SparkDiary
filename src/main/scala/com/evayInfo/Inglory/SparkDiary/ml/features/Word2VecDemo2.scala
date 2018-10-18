package com.evayInfo.Inglory.SparkDiary.ml.features

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{Word2VecModel, Word2Vec}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 18/10/18.
 */
object Word2VecDemo2 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger
    val conf = new SparkConf().setAppName(s"Word2VecDeom2").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

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

    val model_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/Word2VecDemo2"
    model.write.overwrite().save(model_path)

    val load_model = Word2VecModel.load(model_path)
    val word2Vec_test  =load_model.transform(test_df)
    word2Vec_test.show()


    word2VecResult.printSchema()



    sc.stop()
    spark.stop()
  }
}
