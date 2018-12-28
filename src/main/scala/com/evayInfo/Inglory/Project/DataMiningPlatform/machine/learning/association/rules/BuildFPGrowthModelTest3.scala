package com.evayInfo.Inglory.Project.DataMiningPlatform.machine.learning.association.rules

object BuildFPGrowthModelTest3 {

  def main(args: Array[String]) {

    val ipt_table = "FPGrowth_data"
    val col_name = "items"
    val sep = " "
    val support = 0.2
    val confidence = 0.8
    val partitions = 10
    val opt_table = "FPGrowth_data_pred_3"
    val model_path = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/result//fpg_model_3"

    val fpg = new BuildFPGrowthModel()
//    fpg.BuildFPGrowthModel(ipt_table, col_name, sep, support, confidence, partitions, opt_table, model_path)

    var temp = "000"
    println(s"temp is $temp")

//    temp = fpg.BuildFPGrowthModel(ipt_table, col_name, sep, support, confidence, partitions, opt_table, model_path)

//    /*
    try{

      temp = fpg.BuildFPGrowthModel(ipt_table, col_name, sep, support, confidence, partitions, opt_table, model_path)
//      return temp
    } catch {
      case ex: Throwable => println("found a unknown exception"+ ex)
        temp = "002"
//        return temp
    }

//*/
    println("=====================")
    println(s"temp is $temp")

  }

}
