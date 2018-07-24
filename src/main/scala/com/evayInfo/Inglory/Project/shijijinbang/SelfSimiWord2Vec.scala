package com.evayInfo.Inglory.Project.shijijinbang

import org.apache.log4j.{Level, Logger}

/**
 * Created by sunlu on 18/7/23.
 */
object SelfSimiWord2Vec {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {


  }

}
