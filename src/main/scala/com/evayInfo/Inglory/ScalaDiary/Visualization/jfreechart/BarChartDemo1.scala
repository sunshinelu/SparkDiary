package com.evayInfo.Inglory.ScalaDiary.Visualization.jfreechart

import java.awt.Font
import java.io.{FileOutputStream, File}
import javax.swing.JFrame

import org.jfree.chart.axis.{CategoryLabelPositions, ValueAxis, CategoryAxis}
import org.jfree.chart.{ChartUtilities, ChartPanel, ChartFactory}
import org.jfree.chart.plot.{CategoryPlot, PlotOrientation}
import org.jfree.data.category.DefaultCategoryDataset

/**
 * Created by sunlu on 17/11/30.
 */
object BarChartDemo1 {

  def main(args: Array[String]) {
    val x = Array[Double](0.0021, 0.0043,0.0043, 0.0043,0.0086,0.0107, 0.0107,0.0043, 0.0, 0.0 )
    val label = Array[String]("oneDayRetention", "threeDayRetention", "sevenDayRetention", "fifthDayRetention",
    "oneMonthRetention", "twoMonthRetention", "threeMonthRetention", "fourMonthRetention","fiveMonthRetention","sixMonthRetention")
    val dataset = new DefaultCategoryDataset

    val n = x.length
    println("x数组长度为：" + n)
    for(i <- 0 to n-1 ){
      dataset.addValue(x(i), label(i), label(i))
    }

    val barChart = ChartFactory.createBarChart(
      "留存率分析",
      "Category", "留存率(%)",
      dataset,
      PlotOrientation.VERTICAL,
      true, true, false)

    //定义格式
//    val plot: CategoryPlot = barChart.getCategoryPlot
//    val domainAxis: CategoryAxis = plot.getDomainAxis
//    domainAxis.setLabelFont(new Font("黑体", Font.BOLD, 14))
//    domainAxis.setTickLabelFont(new Font("宋体", Font.BOLD, 12))
//    val rangeAxis: ValueAxis = plot.getRangeAxis
//    rangeAxis.setLabelFont(new Font("黑体", Font.BOLD, 15))
//    barChart.getLegend.setItemFont(new Font("黑体", Font.BOLD, 15))
//    barChart.getTitle.setFont(new Font("宋体", Font.BOLD, 20))
//    domainAxis.setCategoryLabelPositions(CategoryLabelPositions.UP_45)

    val frame = new JFrame("Hello BarChartDemo1")
    frame.setDefaultCloseOperation( JFrame.EXIT_ON_CLOSE )

    frame.setSize(640,420)
    frame.add( new ChartPanel(barChart) )
    frame.pack()
    frame.setVisible(true)

    val  width = 640 /* Width of the image */
    val height = 480 /* Height of the image */
    val BarChart = new File( "result/BarChartDemo1.jpeg" )
    ChartUtilities.saveChartAsJPEG( BarChart , barChart , width , height )



  }
}
