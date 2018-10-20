package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.association.rules;

/**
 * Created by sunlu on 18/10/20.
 *  * 输入：
 * model_path:模型所在路径，String类型
 * ipt_table:输入表的表名，String类型
 * col_name:输入列的列名，String类型
 * sep:分隔符，String类型
 * confidence:设置confidence参数，Double类型
 * opt_table:输出表的表名，String类型
 */
public class FPGrowthModelApplicationTest1 {
    public static void main(String[] args) {
        String ipt_table = "FPGrowth_data";
        String col_name = "items";
        String sep = " ";
        Double confidence = 0.8;
        String opt_table = "FPGrowth_data_pred_application";
        String model_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result/fpg_model";

        FPGrowthModelApplication model_application = new FPGrowthModelApplication();
        model_application.FPGrowthModelApplication(model_path, ipt_table, col_name,sep, confidence, opt_table);
    }
}
