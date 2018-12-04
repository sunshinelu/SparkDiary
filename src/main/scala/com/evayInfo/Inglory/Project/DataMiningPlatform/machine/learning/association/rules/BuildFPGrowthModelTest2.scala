package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.association.rules

/**
 * Created by sunlu on 18/10/19.
 *
 * 输入：
 * ipt_table:输入表的表名，String类型
 * col_name:输入数据所在列的列名，String类型
 * sep:设置输入数据之间的分隔符，String类型
 * support:设置support参数，Double类型
 * confidence:设置confidence参数，Double类型
 * partitions:partitions数量，Int类型
 * opt_table:输出表的表名，String类型
 * model_path:模型保存的路径，String类型
 *
 * 输出：FPGrowth模型和预测结果
 *
 */
object BuildFPGrowthModelTest2 {

  def main(args: Array[String]) {

    val ipt_table = "FPGrowth_data"
    val col_name = "items"
    val sep = " "
    val support = 0.2
    val confidence = 0.8
    val partitions = 10
    val opt_table = "FPGrowth_data_pred_2"
    val model_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/fpg_model_2"

    val fpg = new BuildFPGrowthModel()
    fpg.BuildFPGrowthModel(ipt_table, col_name, sep, support, confidence, partitions, opt_table, model_path)

  }
}
