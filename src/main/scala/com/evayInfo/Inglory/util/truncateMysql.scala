package com.evayInfo.Inglory.util

import java.sql.{DriverManager, SQLException}


/**
 * Created by sunlu on 17/6/8.
 * 参考链接：http://database.51cto.com/art/201006/204217.htm
 * http://www.cnblogs.com/hongten/archive/2011/03/29/1998311.html
 * https://examples.javacodegeeks.com/core-java/sql/delete-all-table-rows-example/
 */
object truncateMysql {
  def main(args: Array[String]) {
    /*

        val  connection = null
        try {

          // Load the MySQL JDBC driver

          val driverName = "com.mysql.jdbc.Driver"

          Class.forName(driverName);


          // Create a connection to the database

          val serverName = "localhost";

          val schema = "test";

          val url = "jdbc:mysql://" + serverName +  "/" + schema;

          val username = "username";

          val password = "password";

          connection = DriverManager.getConnection(url, username, password);



          System.out.println("Successfully Connected to the database!");


        } catch (ClassNotFoundException e) {

      System.out.println("Could not find the database driver " + e.getMessage());
        } catch (SQLException e) {

      System.out.println("Could not connect to the database " + e.getMessage());
        }

        try {

          val statement = connection.createStatement();


          /* TRUNCATE is faster than DELETE since

           * it does not generate rollback information and does not

           * fire any delete triggers

           */

          statement.executeUpdate("TRUNCATE test_table");


          System.out.println("Successfully truncated test_table");

        } catch (SQLException e) {

      System.out.println("Could not truncate test_table " + e.getMessage());
        }


    */


    //驱动程序名
    val driver = "com.mysql.jdbc.Driver"
    // URL指向要访问的数据库名scutcs
    val url = "jdbc:mysql://localhost:3306/sunluMySQL"
    // MySQL配置时的用户名
    val user = "root"
    // Java连接MySQL配置时的密码
    val password = "root"
    val tableName = "SPEC_LOG_RECOM"

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

  }

  def truncateMysql(url: String, user: String, password: String, tableName: String): Unit = {
    //驱动程序名
    val driver = "com.mysql.jdbc.Driver"
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

  }

  def truncateMysql2(url: String, user: String, password: String, tableName: String): Unit = {
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

}

