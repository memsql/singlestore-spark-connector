package com.singlestore.spark

import com.singlestore.spark.SQLGen.SQLGenContext
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.metrics.source.MetricsHandler
import org.apache.spark.sql.sources.{
  BaseRelation,
  CreatableRelationProvider,
  DataSourceRegister,
  DataSourceTelemetryProvider,
  RelationProvider
}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

object DefaultSource {

  val SINGLESTORE_SOURCE_NAME          = "com.singlestore.spark"
  val SINGLESTORE_SOURCE_NAME_SHORT    = "singlestore"
  val SINGLESTORE_GLOBAL_OPTION_PREFIX = "spark.datasource.singlestore."

  @Deprecated val MEMSQL_SOURCE_NAME          = "com.memsql.spark"
  @Deprecated val MEMSQL_SOURCE_NAME_SHORT    = "memsql"
  @Deprecated val MEMSQL_GLOBAL_OPTION_PREFIX = "spark.datasource.memsql."
}

class DefaultSource
    extends RelationProvider
    with DataSourceRegister
    with CreatableRelationProvider
    with LazyLogging
    with DataSourceTelemetryProvider {

  override def shortName(): String = DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT

  override def dataSourceType(): String = "spark_connector"

  override def dataWarehouseName(parameters: Map[String, String]): String = shortName()

  private def includeGlobalParams(sqlContext: SQLContext,
                                  params: Map[String, String]): Map[String, String] =
    sqlContext.getAllConfs.foldLeft(params)({
      case (params, (k, v)) if k.startsWith(DefaultSource.SINGLESTORE_GLOBAL_OPTION_PREFIX) =>
        params + (k.stripPrefix(DefaultSource.SINGLESTORE_GLOBAL_OPTION_PREFIX) -> v)
      case (params, (k, v)) if k.startsWith(DefaultSource.MEMSQL_GLOBAL_OPTION_PREFIX) =>
        params + (k.stripPrefix(DefaultSource.MEMSQL_GLOBAL_OPTION_PREFIX) -> v)
      case (params, _) => params
    })

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    val params  = CaseInsensitiveMap(includeGlobalParams(sqlContext, parameters))
    val options = SinglestoreOptions(params, sqlContext.sparkSession.sparkContext)

    initializeRelationTelemetry(sqlContext, parameters)

    if (options.disablePushdown) {
      SQLPushdownRule.ensureRemoved(sqlContext.sparkSession)
      SinglestoreReaderNoPushdown(SinglestoreOptions.getQuery(params), options, sqlContext)
    } else {
      SQLPushdownRule.ensureInjected(sqlContext.sparkSession)
      SinglestoreReader(SinglestoreOptions.getQuery(params),
                        Nil,
                        options,
                        sqlContext,
                        context = SQLGenContext(options, sqlContext.sparkContext))
    }
  }

  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    val opts = CaseInsensitiveMap(includeGlobalParams(sqlContext, parameters))
    val conf = SinglestoreOptions(opts, sqlContext.sparkSession.sparkContext)

    val table = SinglestoreOptions
      .getTable(opts)
      .getOrElse(
        throw new IllegalArgumentException(
          s"To write a dataframe to SingleStore you must specify a table name via the '${SinglestoreOptions.TABLE_NAME}' parameter"
        )
      )
    JdbcHelpers.prepareTableForWrite(conf, table, mode, data.schema)
    val isReferenceTable = JdbcHelpers.isReferenceTable(conf, table)
    val partitionWriterFactory =
      if (conf.onDuplicateKeySQL.isEmpty) {
        new LoadDataWriterFactory(table, conf)
      } else {
        new BatchInsertWriterFactory(table, conf)
      }

    val schema        = data.schema
    var totalRowCount = 0L
    data.foreachPartition((partition: Iterator[Row]) => {
      val writer = partitionWriterFactory.createDataWriter(schema,
                                                           TaskContext.getPartitionId(),
                                                           0,
                                                           isReferenceTable,
                                                           mode)
      try {
        partition.foreach(record => {
          writer.write(record)
          totalRowCount += 1
        })
        writer.commit()
        MetricsHandler.setRecordsWritten(totalRowCount)
      } catch {
        case e: Exception =>
          writer.abort(e)
          throw e
      }
    })

    createRelation(sqlContext, parameters)
  }
}
