package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.recommend

/**
 * Created by sunlu on 18/10/25.
 */
object ALSModelApplicationTest2 {
  def main(args: Array[String]) {


    val user_col = "user"
    val item_col = "item"
    val rating_col = "rating"

    val model_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/als_model_rdd"
    val test_table = "recommenderSys_Demo_Data_sample"
    val opt_table_TopNProductsForUsers = "recommenderSys_Demo_Data_sample_TopNProductsForUsers"
    val opt_table_TopNUsersForProducts = "recommenderSys_Demo_Data_sample_TopNUsersForProducts"

    val model_application = new ALSModelApplication()
    model_application.TopNProductsForUsers(test_table,model_path,user_col,item_col,rating_col,10,opt_table_TopNProductsForUsers)

    model_application.TopNUsersForProducts(test_table,model_path,user_col,item_col,rating_col,10,opt_table_TopNUsersForProducts)


  }

}
