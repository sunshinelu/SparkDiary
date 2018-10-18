package com.evayInfo.Inglory.Project.DataMiningPlatform.text.analysis

/**
 * Created by sunlu on 18/10/18.
 * 测试 BuildFeatureExtractionModel 方法
 */
object BuildFeatureExtractionModelTest2 {

  def main(args: Array[String]) {

    val tableName: String = "Sogou_Classification_mini_segWords_random100"
    val colName: String = "seg_words"
    val feature_size: Integer = 20
    val min_count: Integer = 2
    val model_path: String = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/data_mining_platform/feature_extraction_model"
    val opt_table_WordCount: String = "feature_extraction_WordCount"
    val opt_table_TF_IDF: String = "feature_extraction_TF_IDF"
    val opt_table_Word2Vec: String = "feature_extraction_Word2Vec"

    val feature_model: BuildFeatureExtractionModel = new BuildFeatureExtractionModel

    // 使用词频方法生成特征模型
    feature_model.WordCount(tableName, colName, feature_size, min_count, model_path, opt_table_WordCount)

    // 使用TF_IDF方法生成特征模型
    feature_model.TF_IDF(tableName, colName, feature_size, min_count, model_path, opt_table_TF_IDF)

    // 使用Word2Vec方法生成特征模型
    feature_model.Word2Vec(tableName, colName, feature_size, min_count, model_path, opt_table_Word2Vec)

  }
}
