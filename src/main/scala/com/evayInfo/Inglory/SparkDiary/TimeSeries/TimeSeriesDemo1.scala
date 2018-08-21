package com.evayInfo.Inglory.SparkDiary.TimeSeries

import com.cloudera.sparkts.models.ARIMA
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 18/8/21.
 */
object TimeSeriesDemo1 {

  def forecast(): Unit ={
    val APP_NAME = "Sales Forecast"
    val period = 6

    val conf = new SparkConf().setAppName(APP_NAME).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName(APP_NAME)
      .getOrCreate()

    var dfr = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://my-db-domain:3306/sfdc")
      .option("user", "my-db-username")
      .option("password", "my-db-password")

    val df2 = dfr.option("dbtable","(SELECT CloseDate, Amount FROM toast.opportunity WHERE IsWon='true' AND IsClosed='true') as win").load()
    df2.createOrReplaceTempView("opp")
    val df = df2.sqlContext.sql("SELECT DATE_FORMAT(CloseDate,'yyyy-MM') as CloseDate, SUM(Amount) as Amount FROM opp GROUP BY DATE_FORMAT(CloseDate,'yyyy-MM') ORDER BY CloseDate")
    val monthes = df.collect().flatMap((row: Row) => Array(row.get(0)))
    val amounts = df.collect().flatMap((row: Row) => Array(row.getLong(1).intValue().toDouble))
    {
      // Training
      val trainingSize = (amounts.length * 0.75).toInt
      val trainingAmounts = new Array[Double](trainingSize)
      for(i <- 0 until trainingSize){
        trainingAmounts(i) = amounts(i)
      }

      val actual = new DenseVector(trainingAmounts)
      val period = amounts.length - trainingSize
      val model = ARIMA.autoFit(actual)
      println("best-fit model ARIMA(" + model.p + "," + model.d + "," + model.q + ") AIC=" + model.approxAIC(actual) )
      val predicted = model.forecast(actual, period)
      var totalErrorSquare = 0.0
      for (i <- (predicted.size - period) until predicted.size) {
        val errorSquare = Math.pow(predicted(i) - amounts(i), 2)
        println(monthes(i) + ":\t\t" + predicted(i) + "\t should be \t" + amounts(i) + "\t Error Square = " + errorSquare)
        totalErrorSquare += errorSquare
      }
      println("Root Mean Square Error: " + Math.sqrt(totalErrorSquare/period))
    }

    {
      // Prediction
      val actual = new DenseVector(amounts)
      val model = ARIMA.autoFit(actual)
      println("best-fit model ARIMA(" + model.p + "," + model.d + "," + model.q + ")  AIC=" + model.approxAIC(actual)  )
      val predicted = model.forecast(actual, period)
      for (i <- 0 until predicted.size) {
        println("Model Point #" + i + "\t:\t" + predicted(i))
      }
    }
  }
}
