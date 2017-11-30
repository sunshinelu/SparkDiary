package com.evayInfo.Inglory.ScalaDiary.Visualization.jfreechart

import javax.swing.JFrame

import org.jfree.chart.{ChartPanel, ChartFactory}
import org.jfree.data.general.DefaultPieDataset

/**
 * Created by sunlu on 17/11/30.
 * link:
 * https://marblemice.wordpress.com/2008/10/21/scala-and-jfreechart/
 */
object peiChartDemo1 {
  def main(args: Array[String]) {
    // Create a simple pie chart
    var pieDataset:DefaultPieDataset = new DefaultPieDataset()
    pieDataset.setValue("A", 75)
    pieDataset.setValue("B", 10)
    pieDataset.setValue("C", 10)
    pieDataset.setValue("D", 5)
    val chart = ChartFactory.createPieChart(
      "Hello World",
      pieDataset,
      true,
      true,
      false)

    println("Hello world")

    val frame = new JFrame("Hello Pie World")
    frame.setDefaultCloseOperation( JFrame.EXIT_ON_CLOSE )

    frame.setSize(640,420)
    frame.add( new ChartPanel(chart) )
    frame.pack()
    frame.setVisible(true)

  }
}
