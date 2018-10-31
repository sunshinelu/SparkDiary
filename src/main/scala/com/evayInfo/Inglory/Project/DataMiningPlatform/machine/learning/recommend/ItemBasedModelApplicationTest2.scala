package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.recommend

/**
 * Created by sunlu on 18/10/31.
 */
object ItemBasedModelApplicationTest2 {

  def main(args: Array[String]) {
    //    val ipt_table = "recommenderSys_Demo_Data_sample"
    val ipt_table = "recommender_test"
    val user_col = "user"
    val item_col = "item"
    val rating_col = "rating"
    val simi_threshold = 0.0

    val model_name = "item_simi"
    //    val test_table = "recommenderSys_Demo_Data_sample"
    val test_table = "recommender_test"
    val opt_table = "recommenderSys_Demo_Data_sample_item_based_TopNProductsForUsers"

    val recom = new ItemBasedModelApplication()
    recom.TopNProductsForUsers(test_table,model_name,user_col,item_col,rating_col,10,opt_table)
  }
}
