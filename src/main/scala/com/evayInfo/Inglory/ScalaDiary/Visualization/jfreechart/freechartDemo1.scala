package com.evayInfo.Inglory.ScalaDiary.Visualization.jfreechart

import org.jfree.chart.{ChartFrame, ChartFactory}
import org.jfree.data.xy.DefaultXYDataset

/**
 * Created by sunlu on 17/11/30.
 * https://stackoverflow.com/questions/28516003/plot-scala-arrays-with-jfreechart
 */
object freechartDemo1 {

  def main(args: Array[String]) {

    val x = Array[Double](1,2,3,4,5,6,7,8,9,10)
    val y = x.map(_*2)
    val dataset = new DefaultXYDataset
    dataset.addSeries("Series 1",Array(x,y))

    val frame = new ChartFrame(
      "Title",
      ChartFactory.createScatterPlot(
        "Plot",
        "X Label",
        "Y Label",
        dataset,
        org.jfree.chart.plot.PlotOrientation.HORIZONTAL,
        false,false,false
      )
    )
    frame.pack()
    frame.setVisible(true)

  }
}
