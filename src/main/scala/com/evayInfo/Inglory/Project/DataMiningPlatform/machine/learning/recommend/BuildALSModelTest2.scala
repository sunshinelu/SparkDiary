package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.recommend

/**
 * Created by sunlu on 18/10/25.
 */
object BuildALSModelTest2 {

  def main(args: Array[String]) {

    val ipt_table = "recommenderSys_Demo_Data_sample"
    val user_col = "user"
    val item_col = "item"
    val rating_col = "rating"
    val rank = 10
    val numIterations = 10
    val lambda = 0.01
    val model_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/als_model_rdd"
    val test_table = "recommenderSys_Demo_Data_sample"
    val opt_table = "recommenderSys_Demo_Data_sample_pred"



    val build_model = new BuildALSModel()
    build_model.ALSModel_test(ipt_table,user_col,item_col,rating_col,
      rank,numIterations,lambda,model_path,
      test_table,opt_table)

  }
}
