package com.evayInfo.Inglory.Project.DataMiningPlatform.text.analysis

/**
 * Created by sunlu on 18/10/18.
 */
object FeatureExtractionModelApplicationTest2 {

  def main(args: Array[String]) {

    val colName: String = "seg_words"
    val feature_size: Integer = 20
    val min_count: Integer = 2
    val model_path: String = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/data_mining_platform/feature_extraction_model_word2vec"
    val ipt_train_table = "Sogou_Classification_mini_segWords_random100"
    val opt_train_table_WordCount: String = "feature_extraction_WordCount"
    val ipt_test_table = "Sogou_Classification_mini_segWords_random10"
    val opt_test_table_WordCount: String = "feature_extraction_WordCount_test"

    val feature_model: BuildFeatureExtractionModel = new BuildFeatureExtractionModel

    // 使用词频方法生成特征模型
    feature_model.Word2Vec(ipt_train_table, colName, feature_size, min_count, model_path, opt_train_table_WordCount)

    val model_application:FeatureExtractionModelApplication = new FeatureExtractionModelApplication()

    model_application.FeatureExtractionModel_Word2Vec(model_path, ipt_test_table, opt_test_table_WordCount)

  }
}
