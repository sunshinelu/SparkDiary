package dcjingsai.text

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{Word2Vec, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 18/7/11.
 */
object BulidWord2VecModel {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger
    //bulid environment
    val spark = SparkSession.builder.appName("BulidWord2VecModel").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    val train_file_path = "file:///Users/sunlu/Downloads/new_data/train_set.csv"
//    val train_file_path = "file:///root/lulu/DataSets/new_data/train_set.csv"

    // 读取csv文件，含表头
    val colName_df1 = Seq("id", "article", "word_seg", "class")
    val df1 = spark.read.option("header", true).option("delimiter", ",").
      csv(train_file_path).toDF(colName_df1: _*).randomSplit(Array(0.001, 0.999))(0)

    // 对word_seg中的数据以空格为分隔符转化成seq
    val df2 = df1.select("word_seg").withColumn("word_seg_seq", split($"word_seg"," "))

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec().
      setInputCol("word_seg_seq").
      setOutputCol("word2vec").
      setVectorSize(50).
      setMinCount(3)
    val word2Vec_model = word2Vec.fit(df2)

    val word2Vec_df = word2Vec_model.transform(df2)
    word2Vec_df.show()

    val word2vec_model_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/src/main/scala/dcjingsai/text/word2vec_model_path"
    word2Vec_model.save(word2vec_model_path)

    word2Vec_model.findSynonyms("934612", 5)


/*
    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    wordsData.show()

    print("======")

    val df3 = wordsData.withColumn("col1", split($"sentence"," "))
    df3.show()
    df3.printSchema()

*/

    sc.stop()
    spark.stop()

  }

}
