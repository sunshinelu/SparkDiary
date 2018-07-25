package com.evayInfo.Inglory.Project.shijijinbang

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._

/**
 * Created by sunlu on 18/7/23.
 */
object GenerateList {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  case class ListSchema(id: Int, txt: String)


  def main(args: Array[String]) {

    SetLogger
    //bulid environment
    val spark = SparkSession.builder.appName("SelfSimiCosin").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val file_path = "file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/shijijinbang/self_simi_test1.txt"
    val colName = Seq("txt")
    val df1 = spark.read.textFile(file_path).toDF(colName: _*)
    //    df1.show()

    // 新增一列递增列
    val w1 = Window.orderBy("txt")
    val df2 = df1.withColumn("id", row_number().over(w1))
//    df2.show(false)

    val list1 = df2.rdd.map{case Row(txt:String, id:Int) => (id, txt)}.collect().toList.asJava

    for (i <- list1.asScala) {
      println(i)
    }

    val rdd1 = sc.parallelize(list1.asScala).map(x => ListSchema(x._1,x._2))

    val df3 = spark.createDataset(rdd1)

    df3.show()

    val docSimiDemo1 = new DocSimi()
    val result_list = docSimiDemo1.DocSimiJaccard(list1)

    println ("===============")
    for (i <- result_list.asScala){
      println(i)
    }


    sc.stop()
    spark.stop()
  }

}
