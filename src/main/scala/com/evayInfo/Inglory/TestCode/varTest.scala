package com.evayInfo.Inglory.TestCode

/**
 * Created by sunlu on 17/9/25.
 *
### Java基本类型

Java的数据类型大致可以分为两类：基本（Primitive）类型和对象类型.

基本类型一共有8种，分别是boolean、byte、short、char、int、long、float、double。

为了把基本类型融入OO系统，Java提供了包装类（Wrapper）。
包装类在包java.lang里，一共8个，分别与基本类型一一对应，
它们是：Boolean、Byte、Short、Character、Integer、Long、Float和Double。

### Scala基本类型

和Java的8种基本类型相对应，Scala也定义了8种基本类型，
它们是：Boolean、Byte、Short、Char、Int、Long、Float和Double。

这8种基本类型都定义在scala包里。有趣的是，这8种基本类型虽然有相应的类定义，但是和其他类还是有区别的：
这些类的实例并不是对象，而是直接映射为相应的primitive类型。
 */
object varTest {
  private var a: java.lang.Integer = null
  var b: java.lang.Double = null


  def main(args: Array[String]) {
    a = 9

    println("a is: " + a)

    b = 2.5
    println("b is: " + b)


  }
}
