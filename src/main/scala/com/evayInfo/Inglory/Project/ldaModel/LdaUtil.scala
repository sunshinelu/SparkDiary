package com.evayInfo.Inglory.Project.ldaModel

import org.apache.log4j.{Level, Logger}

object LdaUtil {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


}
