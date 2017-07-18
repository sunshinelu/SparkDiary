### Exception in thread "main" java.sql.SQLException: The server time zone value '�й���׼ʱ��' is unrecognized or represents more than one time zone. You must configure either the server or JDBC driver (via the serverTimezone configuration property) to use a more specifc time zone value if you want to utilize time zone support.


Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties

Tue Jul 18 21:22:05 CST 2017 WARN: Establishing SSL connection without server's identity verification is not recommended. According to MySQL 5.5.45+, 5.6.26+ and 5.7.6+ requirements SSL connection must be established by default if explicit option isn't set. For compliance with existing applications not using SSL the verifyServerCertificate property is set to 'false'. You need either to explicitly disable SSL by setting useSSL=false, or set useSSL=true and provide truststore for server certificate verification.

Exception in thread "main" java.sql.SQLException: The server time zone value '�й���׼ʱ��' is unrecognized or represents more than one time zone. You must configure either the server or JDBC driver (via the serverTimezone configuration property) to use a more specifc time zone value if you want to utilize time zone support.

    val url = "jdbc:mysql://localhost:3306/bbs"

改为：

     val url = "jdbc:mysql://localhost:3306/bbs?useUnicode=true&characterEncoding=UTF-8&" +
          "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
          
参考链接：

http://blog.csdn.net/oppo5630/article/details/52162783

http://www.cnblogs.com/jeffen/p/6288142.html

    <mysql.version>6.0.2</mysql.version>

使用的数据库是MySQL，驱动是6.0.2，这是由于数据库和系统时区差异所造成的，在jdbc连接的url后面加上serverTimezone=GMT即可解决问题，如果需要使用gmt+8时区，需要写成GMT%2B8，否则会被解析为空。再一个解决办法就是使用低版本的mysql jdbc驱动，5.1.28不会存在时区的问题。

为URL添加参数serverTimezone=UTC即可，这里的时区可以根据自己数据库的设定来设置（GMT/UTC ）。
jdbc:mysql://localhost:3306/dbname?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC