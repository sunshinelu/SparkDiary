package com.evayInfo.Inglory.ScalaDiary.Visualization.jfreechart

import java.io.File
import javax.swing.JFrame

import org.jfree.chart.plot.PlotOrientation
import org.jfree.chart.{ChartUtilities, ChartPanel, ChartFactory, ChartFrame}
import org.jfree.data.category.DefaultCategoryDataset
import org.jfree.data.xy.DefaultXYDataset
import org.jfree.ui.RefineryUtilities

/**
 * Created by sunlu on 17/11/30.
 * http://www.yiibai.com/jfreechart/jfreechart_bar_chart.html
 * https://www.tutorialspoint.com/jfreechart/jfreechart_bar_chart.htm
 * https://marblemice.wordpress.com/2008/10/21/scala-and-jfreechart/
 */
object freechartDemo2 {
  def main(args: Array[String]) {


    val x = Array[Double](0.0021, 0.0043,0.0043, 0.0043,0.0086,0.0107, 0.0107,0.0043, 0.0, 0.0 )
    val label = Array[String]("oneDayRetention", "threeDayRetention", "sevenDayRetention", "fifthDayRetention",
    "oneMonthRetention", "twoMonthRetention", "threeMonthRetention", "fourMonthRetention","fiveMonthRetention","sixMonthRetention")
    val dataset = new DefaultCategoryDataset
    dataset.addValue(0.21, "FIAT", "oneDayRetention")
    dataset.addValue(0.43, "FIAT", "threeDayRetention")
    dataset.addValue(0.43, "FIAT", "sevenDayRetention")
    dataset.addValue(0.43, "FIAT", "fifthDayRetention")
    dataset.addValue(0.86, "FIAT", "twoMonthRetention")
    dataset.addValue(1.07, "FIAT", "threeMonthRetention")
    dataset.addValue(0.43, "FIAT", "fourMonthRetention")
    dataset.addValue(0.0, "FIAT", "fiveMonthRetention")
    dataset.addValue(0.0, "FIAT", "sixMonthRetention")

//    val frame = new ChartFrame(
//      "Title",
//      ChartFactory.createScatterPlot(
//        "Plot",
//        "X Label",
//        "Y Label",
//        dataset,
//        org.jfree.chart.plot.PlotOrientation.HORIZONTAL,
//        false,false,false
//      )
//    )
//    frame.pack()
//    frame.setVisible(true)


    val barChart = ChartFactory.createBarChart(
      "留存率分析",
      "Category", "留存率(%)",
      dataset,
      PlotOrientation.VERTICAL,
      true, true, false)
    val frame = new JFrame("Hello Pie World")
    frame.setDefaultCloseOperation( JFrame.EXIT_ON_CLOSE )

    frame.setSize(640,420)
    frame.add( new ChartPanel(barChart) )
    frame.pack()
    frame.setVisible(true)

    val  width = 640 /* Width of the image */
    val height = 480 /* Height of the image */
    val BarChart = new File( "result/BarChart.jpeg" )
    ChartUtilities.saveChartAsJPEG( BarChart , barChart , width , height )


//    val chart = new BarChart_AWT("Car Usage Statistics", "Which car do you like?")
//      chart.pack( )
//    RefineryUtilities.centerFrameOnScreen( chart )
//    chart.setVisible( true )

  }
}
