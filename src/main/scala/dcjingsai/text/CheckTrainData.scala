package dcjingsai.text

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 18/7/11.
 *
 * 1. 查看数据集中的class一共有多少类别，一共有19类
 *
 */
object CheckTrainData {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  def main(args: Array[String]) {
    SetLogger
    //bulid environment
    val spark = SparkSession.builder.appName("CheckTrainData").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val train_file_path = "file:///Users/sunlu/Downloads/new_data/train_set.csv"
    //    val train_file_path = "file:///root/lulu/DataSets/new_data/train_set.csv"

    // 读取csv文件，含表头
    val colName_df1 = Seq("id", "article", "word_seg", "class")
    val df1 = spark.read.option("header", true).option("delimiter", ",").
      csv(train_file_path).toDF(colName_df1: _*)

    val df2 = df1.select("class").dropDuplicates().withColumn("class", $"class".cast("int")).sort("class")

    df2.printSchema()

    df2.show()


    sc.stop()
    spark.stop()

  }

}
