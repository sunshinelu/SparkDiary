package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.recommend;

/**
 * Created by sunlu on 18/10/25.
 */
public class BuildALSModelTest1 {
    public static void main(String[] args) {
        String ipt_table = "recommenderSys_Demo_Data_sample";
        String user_col = "user";
        String item_col = "item";
        String rating_col = "rating";
        Integer rank = 10;
        Integer numIterations = 10;
        Double lambda = 0.01;
        String model_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/als_model_rdd";
        String test_table = "recommenderSys_Demo_Data_sample";
        String opt_table = "recommenderSys_Demo_Data_sample_pred";



        BuildALSModel build_model = new BuildALSModel();
        build_model.ALSModel_test(ipt_table,user_col,item_col,rating_col,
                rank,numIterations,lambda,model_path,
                test_table,opt_table);
    }
}
