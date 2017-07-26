package com.evayInfo.Inglory.util

import java.sql.{DriverManager, SQLException}
import java.util.Properties

import org.apache.spark.sql._

/**
  * Created by sunlu on 17/7/19.
  * 对mysql数据的操作：
  * getMysqlData：获取mysql数据中某表的全部数据
  * deleteMysqlData：根据时间列，删除mysql数据库中的部分数据
  * truncateMysql:清空（truncate）mysql表
  */
class mysqlUtil {

}

object mysqlUtil {

  /*
getMysqlData：获取mysql数据中某表的全部数据
 */
  def getMysqlData(spark: SparkSession, url: String, user: String, password: String, tableName: String): DataFrame = {
    //connect mysql database
    val prop = new Properties()
    prop.setProperty("user", user)
    prop.setProperty("password", password)
    //get data
    val df = spark.read.jdbc(url, tableName, prop)
    df
  }

  /*
deleteMysqlData：根据时间列，删除mysql数据库中的部分数据
 */
  def deleteMysqlData(url: String, user: String, password: String, tableName: String, timeString: String): Unit = {
    //驱动程序名
    val driver = "com.mysql.jdbc.Driver"

    try {
      // 加载驱动程序
      Class.forName(driver)
      // 连续数据库
      val conn = DriverManager.getConnection(url, user, password)
      if (!conn.isClosed())
        System.out.println("Succeeded connecting to the Database!")
      // statement用来执行SQL语句
      val statement = conn.createStatement()
      // 要执行的SQL语句
      //      val sql = "truncate table " + tableName
      val sql = "delete from " + tableName + " where CREATE_DATE < '" + timeString + "'"
      //val sql2 = "delete from ylzx_oper_interest where CREATE_DATE < '2017-06-26 10:15:15'"
      val rs = statement.executeUpdate(sql)
      println("delete data succeeded!")
    } catch {
      case ex: ClassNotFoundException => {
        println("Sorry,can`t find the Driver!")
        ex.printStackTrace()
      }
      case ex: SQLException => {
        ex.printStackTrace()
      }
      case ex: Exception => {
        ex.printStackTrace()
      }

    }

  }

  /*
truncateMysql:清空（truncate）mysql表
 */

  def truncateMysql(url: String, user: String, password: String, tableName: String): Unit = {
    //驱动程序名
    val driver = "com.mysql.jdbc.Driver"

    try {
      // 加载驱动程序
      Class.forName(driver)
      // 连续数据库
      val conn = DriverManager.getConnection(url, user, password)
      if (!conn.isClosed())
        System.out.println("Succeeded connecting to the Database!")
      // statement用来执行SQL语句
      val statement = conn.createStatement()
      // 要执行的SQL语句
      val sql = "truncate table " + tableName
      val rs = statement.executeUpdate(sql)
      println("truncate table succeeded!")
    } catch {
      case ex: ClassNotFoundException => {
        println("Sorry,can`t find the Driver!")
        ex.printStackTrace()
      }
      case ex: SQLException => {
        ex.printStackTrace()
      }
      case ex: Exception => {
        ex.printStackTrace()
      }

    }

  }

  /*
  saveMysqlData：将分析结果保存到数据库中，保存模式有“append”或者“overwrite”

   */
  def saveMysqlData(ds: DataFrame, url: String, user: String, password: String, tableName: String, mode: String): Unit = {
    //connect mysql database
    val prop = new Properties()
    prop.setProperty("user", user)
    prop.setProperty("password", password)
    //save data
    ds.write.mode(mode).jdbc(url, tableName, prop)
  }


}
