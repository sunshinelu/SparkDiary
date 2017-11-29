package com.evayInfo.Inglory.ScalaDiary.Visualization.vegas

import vegas._
import vegas.render.WindowRenderer._

/**
 * Created by sunlu on 17/11/29.
 */
object vegasDemo1 {
  def main(args: Array[String]) {
    val plot = Vegas("Country Pop").
      withData(
        Seq(
          Map("country" -> "USA", "population" -> 314),
          Map("country" -> "UK", "population" -> 64),
          Map("country" -> "DK", "population" -> 80)
        )
      ).
      encodeX("country", Nom).
      encodeY("population", Quant).
      mark(Bar)

    plot.show
    println(plot.toJson)
  }


}
