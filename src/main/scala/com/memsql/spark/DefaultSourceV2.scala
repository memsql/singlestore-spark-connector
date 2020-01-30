package com.memsql.spark

import java.util.Optional
import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{
  DataSourceOptions,
  DataSourceV2,
  ReadSupport,
  SessionConfigSupport,
  WriteSupport
}
import org.apache.spark.sql.types.StructType

object DefaultSourceV2 {
  val MEMSQL_SOURCE_NAME       = "com.memsql.spark.DefaultSourceV2"
  val MEMSQL_SOURCE_NAME_SHORT = "memsql-v2"
  val SUPPORTED_SPARK_VERSION  = "2.3"
}

class DefaultSourceV2
    extends DataSourceV2
    with ReadSupport
    with WriteSupport
    with DataSourceRegister
    with SessionConfigSupport {

  override def keyPrefix(): String = "memsql"
  override def shortName: String   = DefaultSourceV2.MEMSQL_SOURCE_NAME_SHORT

  private def convertOptions(opts: DataSourceOptions): CaseInsensitiveMap[String] =
    CaseInsensitiveMap(opts.asMap().asScala.toMap)

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val opts = convertOptions(options)
    val session = SparkSession.getActiveSession.getOrElse(
      sys.error("failed to get active Spark Session")
    )
    if (!session.version.startsWith(DefaultSourceV2.SUPPORTED_SPARK_VERSION)) {
      sys.error(
        s"com.memsql.spark.DefaultSourceV2 currently only supports Spark version ${DefaultSourceV2.SUPPORTED_SPARK_VERSION}")
    }
    SQLPushdownRule.ensureInjected(session)
    MemsqlReader(opts, session.sqlContext)
  }

  override def createWriter(
      jobId: String,
      schema: StructType,
      mode: SaveMode,
      options: DataSourceOptions
  ): Optional[DataSourceWriter] = {
    val opts = convertOptions(options)
    val conf = MemsqlOptions(opts)

    val table = MemsqlOptions
      .getTable(opts)
      .getOrElse(
        throw new IllegalArgumentException(
          s"To write a dataframe to MemSQL you must specify a table name via the '${MemsqlOptions.TABLE_NAME}' parameter"
        )
      )

    JdbcHelpers.prepareTableForWrite(conf, table, mode, schema)
    Optional.of(new MemsqlWriter(table, conf))
  }
}
