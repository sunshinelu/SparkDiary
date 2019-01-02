package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.recommend

object BuildUserBasedModelTest3 {

  def main(args: Array[String]): Unit = {
    val ipt_table = "recom_data"
    val user_col = "user"
    val item_col = "item"
    val rating_col = "rating"
    val simi_threshold = 0.2

    val model_name = "user_simi"
    val test_table = "recom_data"
    //    val test_table = "recommenderSys_Demo_Data_sample"
    val opt_table_1 = "recom_data_user_based_pred"
//    val opt_table_2 = "recom_data_user_based"


    val recom = new BuildUserBasedModel()
    recom.UserBased(ipt_table,user_col,item_col,rating_col,simi_threshold,model_name)
//    recom.UserBased_test(ipt_table,user_col,item_col,rating_col,simi_threshold,model_name,test_table,opt_table_1)

  }
}
