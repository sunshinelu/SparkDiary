package dcjingsai.text

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{Word2VecModel, Word2Vec}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 18/7/11.
 */
object LoadWord2VecModel {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger
    //bulid environment
    val spark = SparkSession.builder.appName("LoadWord2VecModel").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val word2vec_model_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/src/main/scala/dcjingsai/text/model_path"
    val word2Vec_model = Word2VecModel.load(word2vec_model_path)

    val t1 = word2Vec_model.findSynonyms("934612", 5)



    t1.show()

    sc.stop()
    spark.stop()

  }

}
