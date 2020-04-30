package com.memsql.spark

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.sources.{
  BaseRelation,
  CreatableRelationProvider,
  DataSourceRegister,
  RelationProvider
}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

object DefaultSource {
  val MEMSQL_SOURCE_NAME          = "com.memsql.spark"
  val MEMSQL_SOURCE_NAME_SHORT    = "memsql"
  val MEMSQL_GLOBAL_OPTION_PREFIX = "spark.datasource.memsql."
}

class DefaultSource
    extends RelationProvider
    with DataSourceRegister
    with CreatableRelationProvider
    with LazyLogging {

  override def shortName: String = DefaultSource.MEMSQL_SOURCE_NAME_SHORT

  private def includeGlobalParams(sqlContext: SQLContext,
                                  params: Map[String, String]): Map[String, String] =
    sqlContext.getAllConfs.foldLeft(params)({
      case (params, (k, v)) if k.startsWith(DefaultSource.MEMSQL_GLOBAL_OPTION_PREFIX) =>
        params + (k.stripPrefix(DefaultSource.MEMSQL_GLOBAL_OPTION_PREFIX) -> v)
      case (params, _) => params
    })

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    val params  = CaseInsensitiveMap(includeGlobalParams(sqlContext, parameters))
    val options = MemsqlOptions(params)
    if (options.disablePushdown) {
      SQLPushdownRule.ensureRemoved(sqlContext.sparkSession)
      MemsqlReaderNoPushdown(MemsqlOptions.getQuery(params), options, sqlContext)
    } else {
      SQLPushdownRule.ensureInjected(sqlContext.sparkSession)
      MemsqlReader(MemsqlOptions.getQuery(params), Nil, options, sqlContext)
    }
  }

  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    val opts = CaseInsensitiveMap(includeGlobalParams(sqlContext, parameters))
    val conf = MemsqlOptions(opts)

    val table = MemsqlOptions
      .getTable(opts)
      .getOrElse(
        throw new IllegalArgumentException(
          s"To write a dataframe to MemSQL you must specify a table name via the '${MemsqlOptions.TABLE_NAME}' parameter"
        )
      )

    JdbcHelpers.prepareTableForWrite(conf, table, mode, data.schema)
    val partitionWriterFactory =
      if (conf.onDuplicateKeySQL.isEmpty) {
        new LoadDataWriterFactory(table, conf)
      } else {
        new BatchInsertWriterFactory(table, conf)
      }

    val schema = data.schema
    data.foreachPartition(partition => {
      val writer = partitionWriterFactory.createDataWriter(schema, TaskContext.getPartitionId(), 0)
      try {
        partition.foreach(writer.write)
        writer.commit()
      } catch {
        case e: Exception => {
          writer.abort()
          throw e
        }
      }
    })

    createRelation(sqlContext, parameters)
  }
}
