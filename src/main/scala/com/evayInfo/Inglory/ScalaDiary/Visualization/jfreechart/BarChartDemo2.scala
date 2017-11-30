package com.evayInfo.Inglory.ScalaDiary.Visualization.jfreechart

import java.awt.{GridLayout, Font}
import java.io.{FileOutputStream, File}
import javax.swing.JFrame

import org.jfree.chart.{ChartUtilities, ChartPanel, ChartFactory}
import org.jfree.chart.axis.{CategoryLabelPositions, ValueAxis, CategoryAxis}
import org.jfree.chart.plot.{CategoryPlot, PlotOrientation}
import org.jfree.data.category.DefaultCategoryDataset

/**
 * Created by sunlu on 17/11/30.
 */
object BarChartDemo2 {

  def main(args: Array[String]) {
    val x = Array[Double](0.0021, 0.0043,0.0043, 0.0043,0.0086,0.0107, 0.0107,0.0043, 0.0, 0.0 )
    val label = Array[String]("oneDayRetention", "threeDayRetention", "sevenDayRetention", "fifthDayRetention",
    "oneMonthRetention", "twoMonthRetention", "threeMonthRetention", "fourMonthRetention","fiveMonthRetention","sixMonthRetention")
//    val x = Array[Double](0.0021)
//    val label = Array[String]("oneDayRetention")

    val dataset = new DefaultCategoryDataset()
//    dataset.clear()

    val n = x.length
//    println("x数组长度为：" + n)
    for (i <- 0 to n - 1) {
      dataset.addValue(x(i), label(i), label(i))
      println(i)
    }
//    dataset.addValue(x(0), label(0), label(0))
//    dataset.addValue(x(1), label(1), label(1))


    println(dataset.getColumnCount)
    println(dataset.getRowCount)

    var frame1: ChartPanel = null

    val barChart = ChartFactory.createBarChart3D(
      "留存率分析", // 图表标题
      "Category", // 目录轴的显示标签
      "留存率(%)", // 数值轴的显示标签
      dataset, // 数据集
      PlotOrientation.VERTICAL, // 图表方向：水平、垂直
      true, // 是否显示图例(对于简单的柱状图必须是false)
      true, // 是否生成工具
      false) // 是否生成URL链接

    //定义格式
    val plot: CategoryPlot = barChart.getCategoryPlot
    val domainAxis: CategoryAxis = plot.getDomainAxis
    domainAxis.setLabelFont(new Font("黑体", Font.BOLD, 14))
    domainAxis.setTickLabelFont(new Font("宋体", Font.BOLD, 12))
    val rangeAxis: ValueAxis = plot.getRangeAxis
    rangeAxis.setLabelFont(new Font("黑体", Font.BOLD, 15))
    barChart.getLegend.setItemFont(new Font("黑体", Font.BOLD, 15))
    barChart.getTitle.setFont(new Font("宋体", Font.BOLD, 20))
    domainAxis.setCategoryLabelPositions(CategoryLabelPositions.UP_45)

    frame1 = new ChartPanel(barChart, true)

    val frame = new JFrame("Hello BarChartDemo2")
    frame.setLayout(new GridLayout(1, 1, 10, 10))
    frame.add(frame1)
    frame.setBounds(50, 50, 800, 600)
    frame.pack()
    frame.setVisible(true)

    Thread.sleep(5000)// 解决无法保存图片问题
    /*
参考链接：
http://bbs.csdn.net/topics/360004753
     */
        val filepath = new File("result/BarChartDemo2.jpeg")
        ChartUtilities.saveChartAsJPEG(filepath, barChart, 700, 490)
//    val filepath: File = new File("result/BarChartDemo2.png")
//    val fos_jpg = new FileOutputStream(filepath, false)
//    ChartUtilities.writeChartAsPNG(fos_jpg, barChart, 700, 490)
//    fos_jpg.close

    /*
Thread.sleep(500000)
            val file: File = new File("result/BarChartDemo2.png")
            var fos_jpg: FileOutputStream = null
            try {
              fos_jpg = new FileOutputStream(file, false)
              ChartUtilities.writeChartAsPNG(fos_jpg, barChart, 700, 490)
              fos_jpg.close
            }
            catch {
              case e: Exception => {
                e.printStackTrace
              }
            } finally {
              try {
                fos_jpg.close
              }
              catch {
                case e: Exception => {
                }
              }
            }
*/


  }

}
