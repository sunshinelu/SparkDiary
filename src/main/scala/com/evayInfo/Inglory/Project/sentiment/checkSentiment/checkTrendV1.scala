package com.evayInfo.Inglory.Project.sentiment.checkSentiment

import com.evayInfo.Inglory.util.mysqlUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by sunlu on 17/8/15.
  * 查看t_yq_article表和t_yq_content表中数据不一致的原因
  */

object checkTrendV1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {

    SetLogger

    val conf = new SparkConf().setAppName(s"checkTrendV1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

        val url2 = "jdbc:mysql://localhost:3306/bbs?useUnicode=true&characterEncoding=UTF-8&" +
          "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
//    val url2 = "jdbc:mysql://192.168.37.18:3306/bbs?useUnicode=true&characterEncoding=UTF-8"
    val user2 = "root"
    val password2 = "root"
    val masterTable = "t_yq_article"
    val slaveTable = "t_yq_content"
    val menhuTable = "DA_SEED"
    val allTable = "t_yq_all"

    val articleDF = mysqlUtil.getMysqlData(spark, url2, user2, password2, masterTable) //.select("ARTICLEID")
    val contentDF = mysqlUtil.getMysqlData(spark, url2, user2, password2, slaveTable) //.select("ARTICLEID")
    val allDF = mysqlUtil.getMysqlData(spark, url2, user2, password2, allTable)

    println("articleDF的数量为：" + articleDF.count())//articleDF的数量为：29457
    println("articleDF中ARTICLEID列除重后的数量为：" + articleDF.dropDuplicates(Array("ARTICLEID")).count())
    println("articleDF除重后的数量为：" + articleDF.dropDuplicates().count())
    println("contentDF的数量为:" + contentDF.count)//contentDF的数量为:29456
    println("contentDF除重后的数量为:" + contentDF.dropDuplicates().count) //
    println("allDF的数量为:" + allDF.count)

    /*
    val t_df_1 = articleDF.except(contentDF)
    println("t_df_1的数量为:" + t_df_1.count)//t_df_1的数量为:1

    t_df_1.collect().foreach(println)//[3b27c1f9-d501-4a7b-b77b-0bec7afa6170]

    val df_menhu = dc_menhu.getMenhuData(spark, url2, user2, password2, menhuTable).filter(col("ARTICLEID") === "3b27c1f9-d501-4a7b-b77b-0bec7afa6170")
    df_menhu.collect().foreach(println)
    */

/*
[3b27c1f9-d501-4a7b-b77b-0bec7afa6170,null,关于省经信委领导工作分工的通知,各处室（局）、派出机构、直属单位：经研究，现将省经信委领导工作分工通知如下：一、翁玉耀党组书记、主任：主持委全面工作。二、梁伟新党组副书记、副主任（正厅长级），省国防科工办主任：协助分管国防科技工业、装备工业、规划和投资、军民融合、“一带一路”（国际产能合作）、中国制造2025、自贸区、平潭开放开发、闽台港澳合作和计划财务、人事、党的工作及离退休干部工作。分管投资和规划处、国防科技工业处、装备工业处、计划财务处、人事处、机关党委、离退休干部工作处。三、李长根党组成员、省纪委驻省经信委纪检组组长：负责纪检、监察工作。四、郭学军副主任：协助分管企业改革与发展、中小企业、原材料工业、新型建材与散装水泥发展、办公室运行与后勤保障工作。分管办公室、中小企业处、原材料工业处（省稀土办公室）、省发展新型建筑材料领导小组办公室、省散装水泥办公室、省中小企业服务中心、省经济与信息技术中心、委后勤服务中心。五、陈建业党组成员、副主任：协助分管电子信息产业、软件与服务业、生产服务业、信息化、无线监督管理及工业旅游、工业文化工作。分管电子信息处、软件与服务业处、信息化推进处、生产服务业处、无线电频率台站管理处、无线电监督检查处、各设区市无线电管理局、省电子产品监督检验所、省无线电监测站及各分站。六、李志忠党组成员、总工程师：协助分管经济运行、能源工业、消费品工业、工艺美术发展、电力执法、安全生产监督管理（行业消防）综合协调及食品安全工作。分管经济运行局、能源处（省电力执法办公室）、消费品工业处、省工艺美术发展促进中心。七、兰文党组成员、副主任：协助分管政策法规、技术进步、减轻企业负担、环境和资源综合利用、节能监察、生态环境保护、河长制、行业技术人才和行政审批“三集中”工作。分管政策法规处（省减轻企业负担办公室）、技术进步处、环境和资源综合利用处（省节能监察办公室）、省节能监察（监测）中心、省经济社团服务中心、委行政服务中心。联系省企业与企业家联合会。八、陈传芳党组成员、副主任：协助分管产业协调、石化工业、民企产业项目对接、工业企业去产能、经济技术协作、对口支援工作。分管产业协调处（省民营企业对接办公室）、经济技术协作处（省政府对口支援办公室）、石化工业处（省履行禁止化学武器公约事务办公室）。九、余须东副巡视员：协助分管支前、综合、信息产业工会、特色小镇建设、有关非公组织与行业协会相关工作。分管综合处、支前工作处、信息产业工会。十、陈居雷副巡视员：协助分管产业研究工作。分管产业研究室、省机械科学研究院。福建省经济和信息化委员会2017年7月21日（此件主动公开）,政务,2017-08-15 19:05:47,0,MENHU,null,各处室（局）、派出机构、直属单位：经研究，现将省经信委领导工作分工通知如下：一、翁玉耀党组书记、主任：主持委全面工作。二、梁伟新党组副书记、副主任（正厅长级），省国防科工办主任：协助分管国防科技工业、装备工业、规划和投资、军民融合、“一带一路”（国际产能合作）、中国制造2025、自贸区、平潭开放开发、闽台港澳合作和计划财务、人事、党的工作及离退休干部工作。分管投资和规划处、国防科技工业处、装备工业处、计划财务处、人事处、机关党委、离退休干部工作处。三、李长根党组成员、省纪委驻省经信委纪检组组长：负责纪检、监察工作。四、郭学军副主任：协助分管企业改革与发展、中小企业、原材料工业、新型建材与散装水泥发展、办公室运行与后勤保障工作。分管办公室、中小企业处、原材料工业处（省稀土办公室）、省发展新型建筑材料领导小组办公室、省散装水泥办公室、省中小企业服务中心、省经济与信息技术中心、委后勤服务中心。五、陈建业党组成员、副主任：协助分管电子信息产业、软件与服务业、生产服务业、信息化、无线监督管理及工业旅游、工业文化工作。分管电子信息处、软件与服务业处、信息化推进处、生产服务业处、无线电频率台站管理处、无线电监督检查处、各设区市无线电管理局、省电子产品监督检验所、省无线电监测站及各分站。六、李志忠党组成员、总工程师：协助分管经济运行、能源工业、消费品工业、工艺美术发展、电力执法、安全生产监督管理（行业消防）综合协调及食品安全工作。分管经济运行局、能源处（省电力执法办公室）、消费品工业处、省工艺美术发展促进中心。七、兰文党组成员、副主任：协助分管政策法规、技术进步、减轻企业负担、环境和资源综合利用、节能监察、生态环境保护、河长制、行业技术人才和行政审批“三集中”工作。分管政策法规处（省减轻企业负担办公室）、技术进步处、环境和资源综合利用处（省节能监察办公室）、省节能监察（监测）中心、省经济社团服务中心、委行政服务中心。联系省企业与企业家联合会。八、陈传芳党组成员、副主任：协助分管产业协调、石化工业、民企产业项目对接、工业企业去产能、经济技术协作、对口支援工作。分管产业协调处（省民营企业对接办公室）、经济技术协作处（省政府对口支援办公室）、石化工业处（省履行禁止化学武器公约事务办公室）。九、余须东副巡视员：协助分管支前、综合、信息产业工会、特色小镇建设、有关非公组织与行业协会相关工作。分管综合处、支前工作处、信息产业工会。十、陈居雷副巡视员：协助分管产业研究工作。分管产业研究室、省机械科学研究院。福建省经济和信息化委员会2017年7月21日（此件主动公开）]

 */
    val t1_df = allDF.filter(col("ARTICLEID") === "3b27c1f9-d501-4a7b-b77b-0bec7afa6170")
    //ff6d9232-6a4d-4178-b892-5d7c7211b34d


    /*
    将结果保存到MySQL中：jdbc at mysqlUtil.scala:119

     */
    /*
    contentDF.printSchema()

    val t2_df = contentDF.withColumn("value", lit(1)).groupBy("articleId", "content").agg(sum("value")).filter($"sum(value)" > 1)
    println(t2_df.count)

    val t3_df = articleDF.withColumn("value", lit(1)).groupBy("articleId").agg(sum("value")).filter($"sum(value)" > 1)
    println(t3_df.count)
*/
    sc.stop()
    spark.stop()
  }

}
