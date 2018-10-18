package com.evayInfo.Inglory.Project.DataMiningPlatform.text.analysis;

import java.io.IOException;

/**
 * Created by sunlu on 18/10/18.
 */
public class FeatureExtractionModelApplicationTest1 {
    public static void main(String[] args) throws IOException {
        String colName = "seg_words";
        Integer feature_size = 20;
        Integer min_count = 2;
        String model_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/data_mining_platform/feature_extraction_model_word_count";
        String ipt_train_table = "Sogou_Classification_mini_segWords_random100";
        String opt_train_table_WordCount = "feature_extraction_WordCount";
        String ipt_test_table = "Sogou_Classification_mini_segWords_random10";
        String opt_test_table_WordCount = "feature_extraction_WordCount_test";

        BuildFeatureExtractionModel feature_model = new BuildFeatureExtractionModel();

        // 使用词频方法生成特征模型(可使用的方法有：WordCount, TF_IDF, Word2Vec)
        feature_model.Word2Vec(ipt_train_table, colName, feature_size, min_count, model_path, opt_train_table_WordCount);

        FeatureExtractionModelApplication model_application = new FeatureExtractionModelApplication();

        // 对模型进行应用(可调用的模型有：WordCount, TF_ID， Word2Vec)
        model_application.FeatureExtractionModel_Word2Vec(model_path, ipt_test_table, opt_test_table_WordCount);
    }
}
