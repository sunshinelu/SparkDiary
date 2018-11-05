package com.evayInfo.Inglory.Project.DataMiningPlatform.utils;

/**
 * Created by sunlu on 18/11/5.
 */
public interface Constants {


    /** 项目配置相关常量*/
    /** Mysql 配置*/
    String MYSQL_JDBC_DRIVER = "mysql.jdbc.driver";
    String MYSQL_JDBC_URL = "mysql.jdbc.url";
    String MYSQL_JDBC_USER = "mysql.jdbc.user";
    String MYSQL_JDBC_PASSWORD = "mysql.jdbc.password";
    String MYSQL_JDBC_DATASOURCE_SIZE = "mysql.jdbc.datasource.size";

    /** python接口的url*/
    String PYTHON_URL = "python.url";
    String PYTHON_MISSING_VALUE_URL = "python.missing.value.url";
    String PYTHON_ABNORMAL_VALUE_URL = "python.abnormal.value.url";
    String PYTHON_TS_ARIMA_URL = "python.ts.arima.url";

    /** hdfs 地址*/
    String HDFS_ADDRESS = "hdfs.address";

    /** Oracle 配置*/
    String ORACLE_JDBC_DRIVER = "oracle.jdbc.driver";
    String ORACLE_JDBC_URL = "oracle.jdbc.url";
    String ORACLE_JDBC_USER = "oracle.jdbc.user";
    String ORACLE_JDBC_PASSWORD = "oracle.jdbc.password";

    /** SQL Server 配置*/
    String SQLSERVER_JDBC_DRIVER = "sqlserver.jdbc.driver";
    String SQLSERVER_JDBC_URL = "sqlserver.jdbc.url";
    String SQLSERVER_JDBC_USER = "sqlserver.jdbc.user";
    String SQLSERVER_JDBC_PASSWORD = "sqlserver.jdbc.password";

    /** hive 配置*/
    String HIVE_URL = "hive.url";
    String HIVE_USER = "hive.user";
    String HIVE_PASSWORD = "hive.password";

    /** hbase 配置*/
    String HBASE_URL = "hbase.url";
    String HBASE_PORT = "hbase.port";

    /** redis 配置*/
    String REDIS_URL = "redis.url";
    String REDIS_PORT = "redis.port";
    String REDIS_PASSWORD = "redis.password";

    /** 音视频分析url*/
    /** 语音转文本*/
    String AUDIOVIDEO_VIDEO2TEXT = "audiovideo.video2text";
    /** 图像分类*/
    String AUDIOVIDEO_PICTURE_CLASSIFY = "audiovideo.picture.classify";
    /** 目标检测*/
    String AUDOIOVIDEO_TARGET_DETECTION = "audiovideo.target.detection";




}
