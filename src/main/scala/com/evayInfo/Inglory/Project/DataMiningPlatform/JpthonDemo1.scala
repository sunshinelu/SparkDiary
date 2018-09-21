package com.evayInfo.Inglory.Project.DataMiningPlatform

import org.python.util.PythonInterpreter

/**
 * Created by sunlu on 18/9/21.
 */
object JpthonDemo1 {
  def main(args: Array[String]) {
    val interpreter = new PythonInterpreter()
    interpreter.exec("print('hello')")
  }

}
