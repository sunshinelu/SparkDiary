package com.evayInfo.Inglory.Project.Recommend

import org.apache.log4j.{Level, Logger}

object RecomUtil {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }



}
