package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.recommend

/**
 * Created by sunlu on 18/10/31.
 */
object BuildUserBasedModelTest2 {

  def main(args: Array[String]) {
//    val ipt_table = "recommenderSys_Demo_Data_sample"
    val ipt_table = "recommender_test"
    val user_col = "user"
    val item_col = "item"
    val rating_col = "rating"
    val simi_threshold = 0.0

    val model_name = "user_simi"
    val test_table = "recommender_test"
    val opt_table = "recommenderSys_Demo_Data_sample_pred_user_based"

    val recom = new BuildUserBasedModel()
    recom.UserBased_test(ipt_table,user_col,item_col,rating_col,simi_threshold,model_name,test_table,opt_table)

  }
}
