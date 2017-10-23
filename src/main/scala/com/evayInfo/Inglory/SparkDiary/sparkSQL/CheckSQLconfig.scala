package com.evayInfo.Inglory.SparkDiary.sparkSQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/10/23.
 * 查看当前环境SQL参数的配置
 * 参考链接：
 * Spark2 SQL configuration参数配置：
 * http://www.cnblogs.com/wwxbi/p/6114410.html
 */
object CheckSQLconfig {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    //bulid environment
    val SparkConf = new SparkConf().setAppName(s"CheckSQLconfig").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(SparkConf).getOrCreate()
    val sc = spark.sparkContext

    spark.sql("SET -v").collect().foreach(println)
    /*
    [spark.sql.sources.parallelPartitionDiscovery.threshold,32,The maximum number of files allowed for listing files at driver side. If the number of detected files exceeds this value during partition discovery, it tries to list the files with another Spark distributed job. This applies to Parquet, ORC, CSV, JSON and LibSVM data sources.]
    [spark.sql.shuffle.partitions,200,The default number of partitions to use when shuffling data for joins or aggregations.]
    [spark.sql.hive.metastorePartitionPruning,true,When true, some predicates will be pushed down into the Hive metastore so that unmatching partitions can be eliminated earlier. This only affects Hive tables not converted to filesource relations (see HiveUtils.CONVERT_METASTORE_PARQUET and HiveUtils.CONVERT_METASTORE_ORC for more information).]
    [spark.sql.broadcastTimeout,300,Timeout in seconds for the broadcast wait time in broadcast joins.]
    [spark.sql.sources.bucketing.enabled,true,When false, we will treat bucketed table as normal table]
    [spark.sql.optimizer.metadataOnly,true,When true, enable the metadata-only query optimization that use the table's metadata to produce the partition columns instead of table scans. It applies when all the columns scanned are partition columns and the query has an aggregate operator that satisfies distinct semantics.]
    [spark.sql.streaming.metricsEnabled,false,Whether Dropwizard/Codahale metrics will be reported for active streaming queries.]
    [spark.sql.parquet.filterPushdown,true,Enables Parquet filter push-down optimization when set to true.]
    [spark.sql.statistics.fallBackToHdfs,false,If the table statistics are not available from table metadata enable fall back to hdfs. This is useful in determining if a table is small enough to use auto broadcast joins.]
    [spark.sql.adaptive.enabled,false,When true, enable adaptive query execution.]
    [spark.sql.parquet.cacheMetadata,true,Turns on caching of Parquet schema metadata. Can speed up querying of static data.]
    [spark.sql.parquet.respectSummaryFiles,false,When true, we make assumption that all part-files of Parquet are consistent with summary files and we will ignore them when merging schema. Otherwise, if this is false, which is the default, we will merge all part-files. This should be considered as expert-only option, and shouldn't be enabled before knowing what it means exactly.]
    [spark.sql.warehouse.dir,file:/Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/spark-warehouse/,The default location for managed databases and tables.]
    [spark.sql.orderByOrdinal,true,When true, the ordinal numbers are treated as the position in the select list. When false, the ordinal numbers in order/sort by clause are ignored.]
    [spark.sql.groupByOrdinal,true,When true, the ordinal numbers in group by clauses are treated as the position in the select list. When false, the ordinal numbers are ignored.]
    [spark.sql.thriftserver.scheduler.pool,<undefined>,Set a Fair Scheduler pool for a JDBC client session.]
    [spark.sql.orc.filterPushdown,false,When true, enable filter pushdown for ORC files.]
    [spark.sql.adaptive.shuffle.targetPostShuffleInputSize,67108864b,The target post-shuffle input size in bytes of a task.]
    [spark.sql.sources.default,parquet,The default data source to use in input/output.]
    [spark.sql.parquet.compression.codec,snappy,Sets the compression codec use when writing Parquet files. Acceptable values include: uncompressed, snappy, gzip, lzo.]
    [spark.sql.crossJoin.enabled,false,When false, we will throw an error if a query contains a cartesian product without explicit CROSS JOIN syntax.]
    [spark.sql.parquet.writeLegacyFormat,false,Whether to follow Parquet's format specification when converting Parquet schema to Spark SQL schema and vice versa.]
    [spark.sql.hive.verifyPartitionPath,false,When true, check all the partition paths under the table's root directory when reading data stored in HDFS.]
    [spark.sql.streaming.numRecentProgressUpdates,100,The number of progress updates to retain for a streaming query]
    [spark.sql.variable.substitute,true,This enables substitution using syntax like ${var} ${system:var} and ${env:var}.]
    [spark.sql.thriftserver.ui.retainedStatements,200,The number of SQL statements kept in the JDBC/ODBC web UI history.]
    [spark.sql.parquet.enableVectorizedReader,true,Enables vectorized parquet decoding.]
    [spark.sql.parquet.mergeSchema,false,When true, the Parquet data source merges schemas collected from all data files, otherwise the schema is picked from the summary file or a random data file if no summary file is available.]
    [spark.sql.parquet.binaryAsString,false,Some other Parquet-producing systems, in particular Impala and older versions of Spark SQL, do not differentiate between binary data and strings when writing out the Parquet schema. This flag tells Spark SQL to interpret binary data as a string to provide compatibility with these systems.]
    [spark.sql.hive.manageFilesourcePartitions,true,When true, enable metastore partition management for file source tables as well. This includes both datasource and converted Hive tables. When partition managment is enabled, datasource tables store partition in the Hive metastore, and use the metastore to prune partitions during query planning.]
    [spark.sql.files.ignoreCorruptFiles,false,Whether to ignore corrupt files. If true, the Spark jobs will continue to run when encountering corrupted or non-existing and contents that have been read will still be returned.]
    [spark.sql.columnNameOfCorruptRecord,_corrupt_record,The name of internal column for storing raw/un-parsed JSON records that fail to parse.]
    [spark.sql.files.maxPartitionBytes,134217728,The maximum number of bytes to pack into a single partition when reading files.]
    [spark.sql.streaming.checkpointLocation,<undefined>,The default location for storing checkpoint data for streaming queries.]
    [spark.sql.hive.filesourcePartitionFileCacheSize,262144000,When nonzero, enable caching of partition file metadata in memory. All tables share a cache that can use up to specified num bytes for file metadata. This conf only has an effect when hive filesource partition management is enabled.]
    [spark.sql.parquet.int96AsTimestamp,true,Some Parquet-producing systems, in particular Impala, store Timestamp into INT96. Spark would also store Timestamp as INT96 because we need to avoid precision lost of the nanoseconds field. This flag tells Spark SQL to interpret INT96 data as a timestamp to provide compatibility with these systems.]
    [spark.sql.autoBroadcastJoinThreshold,10485760,Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when performing a join.  By setting this value to -1 broadcasting can be disabled. Note that currently statistics are only supported for Hive Metastore tables where the command <code>ANALYZE TABLE &lt;tableName&gt; COMPUTE STATISTICS noscan</code> has been run, and file-based data source tables where the statistics are computed directly on the files of data.]
    [spark.sql.pivotMaxValues,10000,When doing a pivot without specifying values for the pivot column this is the maximum number of (distinct) values that will be collected without error.]
    [spark.sql.sources.partitionColumnTypeInference.enabled,true,When true, automatically infer the data types for partitioned columns.]
    [spark.sql.thriftserver.ui.retainedSessions,200,The number of SQL client sessions kept in the JDBC/ODBC web UI history.]
     */
    sc.stop()
    spark.stop()
  }

}
