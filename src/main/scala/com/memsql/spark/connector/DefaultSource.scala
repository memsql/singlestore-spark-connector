package com.memsql.spark.connector

import com.memsql.spark.connector.sql.TableIdentifier
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}

/**
  * This class is a proxy for the actual implementation in org.apache.spark.
  * It allows you to write data to MemSQL via the Spark RelationProvider API.
  *
  * Example:
  *   df.write.format("com.memsql.spark.connector").save("foo.bar")
  */
class DefaultSource extends RelationProvider
  with CreatableRelationProvider with DataSourceRegister{

  override def shortName(): String = "memsql"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    val conf = MemSQLConf(sqlContext.sparkContext.getConf)
    val cluster = MemSQLCluster(conf)

    parameters.get("path") match {
      case Some(path) => MemSQLTableRelation(cluster, DefaultSource.getTableIdentifier(path), sqlContext)

      case None => parameters.get("query") match {
        case Some(query) => MemSQLQueryRelation(cluster, query, sqlContext)
        case None => throw new UnsupportedOperationException("Must specify a path or query when loading a MemSQL DataSource")
      }
    }
  }

  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame
                             ): BaseRelation = {
    val conf = MemSQLConf(sqlContext.sparkContext.getConf)
    val cluster = MemSQLCluster(conf)
    val saveConf = SaveToMemSQLConf(conf, Some(mode), parameters)

    val path = parameters.get("path").getOrElse("")
    if (path == "") {
      throw new UnsupportedOperationException("Must specify a path when saving to MemSQL")
    }

    val tableIdent = DefaultSource.getTableIdentifier(path)
    val relation = MemSQLTableRelation(cluster, tableIdent, sqlContext)
    relation.insert(data, saveConf)

    relation
  }
}

object DefaultSource {
  def getTableIdentifier(path: String): TableIdentifier = {
    val sparkTableIdent = CatalystSqlParser.parseTableIdentifier(path)
    TableIdentifier(sparkTableIdent.table, sparkTableIdent.database)
  }
}
