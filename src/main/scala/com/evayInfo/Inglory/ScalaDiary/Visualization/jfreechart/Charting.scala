package com.evayInfo.Inglory.ScalaDiary.Visualization.jfreechart

import org.jfree.chart.{ChartFactory, ChartPanel}
import org.jfree.chart.plot.PlotOrientation
import org.jfree.data.category.DefaultCategoryDataset
import org.jfree.ui.{ApplicationFrame, RefineryUtilities}


/**
 * Created by sunlu on 17/11/29.
 * 参考链接：
 * Spark之路 --- Scala用JFreeChart画图表实例
 * http://www.cnblogs.com/keitsi/p/spark_scala_charting_demo.html
 *
 */
object RunningCharting {
  def main(args: Array[String]) {
    val chart = new Charting("Testing Yield",
      "Yield of Testing vs Hours")
    chart.pack();
    RefineryUtilities.centerFrameOnScreen(chart);
    chart.setVisible(true);
  }
}

class Charting(applicationTitle: String) extends ApplicationFrame(applicationTitle) {

  def this(applicationTitle: String, chartTitle: String) {
    this(applicationTitle);

    val lineChart = ChartFactory.createLineChart(chartTitle,
      "Hours", "Yield of Testing", createDataset(),
      PlotOrientation.VERTICAL, true, true, false)

    val chartPanel = new ChartPanel(lineChart)
    chartPanel.setPreferredSize(new java.awt.Dimension(560, 367))
    setContentPane(chartPanel)
  }

  def createDataset(): DefaultCategoryDataset = {
    val dataset = new DefaultCategoryDataset();
    dataset.addValue(81, "Yield", "10");
    dataset.addValue(90, "Yield", "11");
    dataset.addValue(78, "Yield", "12");
    dataset.addValue(85, "Yield", "13");
    dataset.addValue(82, "Yield", "14");
    dataset.addValue(80, "Yield", "15");
    return dataset;
  }
}
