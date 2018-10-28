package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.recommend;

/**
 * Created by sunlu on 18/10/25.
 */
public class ALSModelApplicationTest1 {
    public static void main(String[] args) {
        String user_col = "user";
        String item_col = "item";
        String rating_col = "rating";

        String model_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/als_model_rdd";
        String test_table = "recommenderSys_Demo_Data_sample";
        String opt_table_TopNProductsForUsers = "recommenderSys_Demo_Data_sample_TopNProductsForUsers";
        String opt_table_TopNUsersForProducts = "recommenderSys_Demo_Data_sample_TopNUsersForProducts";

        ALSModelApplication model_application = new ALSModelApplication();

        // 向用户推荐商品
        model_application.TopNProductsForUsers(test_table,model_path,user_col,item_col,rating_col,10,opt_table_TopNProductsForUsers);

        // 根据商品推荐用户
        model_application.TopNUsersForProducts(test_table,model_path,user_col,item_col,rating_col,10,opt_table_TopNUsersForProducts);

    }
}
