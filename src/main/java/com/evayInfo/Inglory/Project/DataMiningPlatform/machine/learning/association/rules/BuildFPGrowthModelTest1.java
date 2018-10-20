package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.association.rules;

/**
 * Created by sunlu on 18/10/20.
 *  * 输入：
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
 */
public class BuildFPGrowthModelTest1 {
    public static void main(String[] args) {
        String ipt_table = "FPGrowth_data";
        String col_name = "items";
        String sep = " ";
        Double support = 0.2;
        Double confidence = 0.8;
        Integer partitions = 10;
        String opt_table = "FPGrowth_data_pred";
        String model_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/fpg_model";

        BuildFPGrowthModel fpg = new BuildFPGrowthModel();
        fpg.BuildFPGrowthModel(ipt_table, col_name, sep, support, confidence, partitions, opt_table, model_path);
    }
}
