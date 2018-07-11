package dcjingsai.text

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.feature.{IndexToString, Word2Vec, StringIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 18/7/11.
 *
 * 使用MultilayerPerceptronClassifier分类器构建多分类模型
 *
 */
object MPClassifier {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger
    //bulid environment
    val spark = SparkSession.builder.appName("MPClassifier").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val train_file_path = "file:///Users/sunlu/Downloads/new_data/train_set.csv"
    val test_file_path = "file:///Users/sunlu/Downloads/new_data/test_set.csv"
    val word2vec_model_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/src/main/scala/dcjingsai/text/model_path"

    // 读取csv文件，含表头
    val colName_train = Seq("id", "article", "word_seg", "class")
    val train_df1 = spark.read.option("header", true).option("delimiter", ",").
      csv(train_file_path).toDF(colName_train: _*)

    val colName_test = Seq("id", "article", "word_seg")
    val test_df1 = spark.read.option("header", true).option("delimiter", ",").
      csv(test_file_path).toDF(colName_test: _*)


    // 对word_seg中的数据以空格为分隔符转化成seq
    val train_df2 = train_df1.withColumn("word_seg_seq", split($"word_seg"," "))
    val test_df2 = test_df1.withColumn("word_seg_seq", split($"word_seg"," "))

    val layers = Array[Int](50, 10, 8, 19)

    val labelIndexer = new StringIndexer()
      .setInputCol("class")
      .setOutputCol("label")
      .fit(train_df2)
    val train_df3 = labelIndexer.transform(train_df2)


    // 使用word2vec将文本转化为数值型词向量
    val word2vec = new Word2Vec().
      setInputCol("word_seg_seq").
      setOutputCol("featuresCol").
      setVectorSize(50).
      setMaxIter(3)
    val word2vecModel = word2vec.fit(train_df3)

    val word2vec_train_df = word2vecModel.transform(train_df3)
    val word2vec_test_df = word2vecModel.transform(test_df2)



    // 使用 MultilayerPerceptronClassifier 训练一个多层感知器模型
    val mpc = new MultilayerPerceptronClassifier().
      setLayers(layers).
      setBlockSize(512).
      setSeed(1234L).
      setMaxIter(128).
      setLabelCol("label").
      setFeaturesCol("featuresCol").
      setPredictionCol("predictionCol")
    val mpcModel = mpc.fit(word2vec_train_df)

    val predictions_test = mpcModel.transform(word2vec_test_df)


    // 使用 LabelConverter 将预测结果的数值标签转换成原始的文本标签
    val labelConverter = new IndexToString().
      setInputCol("predictionCol").
      setOutputCol("predictedLabelCol").
      setLabels(labelIndexer.labels)
    val predictionsLabel = labelConverter.transform(predictions_test)


    sc.stop()
    spark.stop()

  }

}
