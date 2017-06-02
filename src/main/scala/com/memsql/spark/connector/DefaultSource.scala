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
    val conf = sqlContext.sparkSession.memSQLConf
    val cluster = MemSQLCluster(conf)
    val disablePartitionPushdown: Boolean = {
      conf.disablePartitionPushdown || DefaultSource.getValueAsBoolean(parameters, "disablePartitionPushdown")
    }
    val enableStreaming: Boolean = DefaultSource.getValueAsBoolean(parameters, "enableStreaming")

    parameters.get("path") match {
      case Some(path) => MemSQLTableRelation(cluster, DefaultSource.getTableIdentifier(path), sqlContext, disablePartitionPushdown, enableStreaming)

      case None => parameters.get("query") match {
        case Some(query) => MemSQLQueryRelation(cluster, query, parameters.get("database"), sqlContext, disablePartitionPushdown, enableStreaming)
        case None => throw new UnsupportedOperationException("Must specify a path or query when loading a MemSQL DataSource")
      }
    }
  }

  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame
                             ): BaseRelation = {
    val conf = sqlContext.sparkSession.memSQLConf
    val cluster = MemSQLCluster(conf)
    val saveConf = SaveToMemSQLConf(conf, Some(mode), parameters)
    val disablePartitionPushdown: Boolean = {
      conf.disablePartitionPushdown || DefaultSource.getValueAsBoolean(parameters, "disablePartitionPushdown")
    }
    val enableStreaming: Boolean = DefaultSource.getValueAsBoolean(parameters, "enableStreaming")

    val path = parameters.get("path").getOrElse("")
    if (path == "") {
      throw new UnsupportedOperationException("Must specify a path when saving to MemSQL")
    }

    val tableIdent = DefaultSource.getTableIdentifier(path)
    val relation = MemSQLTableRelation(cluster, tableIdent, sqlContext, disablePartitionPushdown, enableStreaming)
    relation.insert(data, saveConf)

    relation
  }
}

object DefaultSource {
  def getTableIdentifier(path: String): TableIdentifier = {
    val sparkTableIdent = CatalystSqlParser.parseTableIdentifier(path)
    TableIdentifier(sparkTableIdent.table, sparkTableIdent.database)
  }

  def getValueAsBoolean(parameters: Map[String, String], key: String): Boolean = {
    parameters.get(key) match {
      case None => false
      case Some(s) => {
        s.toLowerCase.trim match {
          case "true" => true
          case _ => false
        }
      }
    }
  }
}
