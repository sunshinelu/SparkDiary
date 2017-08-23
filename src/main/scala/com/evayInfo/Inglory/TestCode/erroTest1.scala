package com.evayInfo.Inglory.TestCode

/**
 * Created by sunlu on 17/8/23.
 */
object erroTest1 {


  def main(args: Array[String]) {
    import java.util.Properties
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123456")
    //    write.mode("append").jdbc("jdbc:mysql://10.20.5.128:3306/talentscout?useUnicode=true&characterEncoding=UTF-8",
    //      "test_002",props)
  }


}
