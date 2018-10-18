package com.evayInfo.Inglory.Project.DataMiningPlatform.text.analysis;


import java.io.IOException;


/**
 * Created by sunlu on 18/10/17.
 */
public class BuildFeatureExtractionModelTest1 {
    public static void main(String[] args) throws IOException {

        String tableName = "Sogou_Classification_mini_segWords_random100" ;// 待分析表名，string类型
        String colName = "seg_words"; // 分词后所在列列名，类型
        Integer feature_size =  20;// 特征长度，Int类型
        Integer min_count = 2; // 最小词频数，Int类型
        String model_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/data_mining_platform/feature_extraction_model";// 模型所在路径，String类型
        String opt_table_WordCount = "feature_extraction_WordCount";// 输出表名，String类型
        String opt_table_TF_IDF = "feature_extraction_TF_IDF";// 输出表名，String类型
        String opt_table_Word2Vec = "feature_extraction_Word2Vec";// 输出表名，String类型


        BuildFeatureExtractionModel feature_model = new BuildFeatureExtractionModel();

        // 使用词频方法生成特征模型
        feature_model.WordCount(tableName, colName, feature_size,min_count, model_path, opt_table_WordCount);

        // 使用TF_IDF方法生成特征模型
        feature_model.TF_IDF(tableName, colName, feature_size, min_count, model_path, opt_table_TF_IDF);

        // 使用Word2Vec方法生成特征模型
        feature_model.Word2Vec(tableName, colName, feature_size, min_count, model_path, opt_table_Word2Vec);

    }
}
