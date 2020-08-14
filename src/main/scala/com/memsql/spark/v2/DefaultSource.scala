package com.memsql.spark.v2

import java.util

import com.memsql.spark.SQLGen.SQLGenContext
import com.memsql.spark.{DefaultSource, JdbcHelpers, MemsqlOptions}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{
  Identifier,
  SupportsCatalogOptions,
  Table,
  TableProvider
}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

class DefaultSource extends TableProvider with DataSourceRegister with SupportsCatalogOptions {

  override def extractIdentifier(options: CaseInsensitiveStringMap): Identifier = {
    new MemsqlIdentifier()
  }

  override def shortName(): String = "memsql"

  private def includeGlobalParams(sqlContext: SparkContext,
                                  params: Map[String, String]): Map[String, String] =
    sqlContext.getConf.getAll.foldLeft(params)({
      case (params, (k, v)) if k.startsWith(DefaultSource.MEMSQL_GLOBAL_OPTION_PREFIX) =>
        params + (k.stripPrefix(DefaultSource.MEMSQL_GLOBAL_OPTION_PREFIX) -> v)
      case (params, _) => params
    })

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    getTable(options).schema()
  }

  override def getTable(schema: StructType,
                        partitioning: Array[Transform],
                        properties: util.Map[String, String]): Table = {
    getTable(new CaseInsensitiveStringMap(properties), Some(schema))
  }

  private def getTable(options: CaseInsensitiveStringMap,
                       schema: Option[StructType] = None): Table = {
    val sparkSession = SparkSession.active
    val opts = CaseInsensitiveMap(
      includeGlobalParams(sparkSession.sparkContext, options.asCaseSensitiveMap().asScala.toMap))
    val conf = MemsqlOptions(opts)
    val query =
      MemsqlOptions.getQuery(CaseInsensitiveMap[String](options.asCaseSensitiveMap().asScala.toMap))
    val table = MemsqlOptions
      .getTable(opts)
      .getOrElse(
        throw new IllegalArgumentException(
          s"To write a dataframe to MemSQL you must specify a table name via the '${MemsqlOptions.TABLE_NAME}' parameter"
        )
      )
    MemsqlTable(query,
                conf,
                sparkSession.sparkContext,
                table,
                schema,
                context = SQLGenContext(conf))
  }
}
