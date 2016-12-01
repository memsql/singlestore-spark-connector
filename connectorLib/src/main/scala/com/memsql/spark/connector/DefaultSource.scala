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
    val tableIdent = DefaultSource.getTableIdentifier(parameters)

    MemSQLTableRelation(cluster, tableIdent, sqlContext)
  }

  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame
                             ): BaseRelation = {
    val conf = MemSQLConf(sqlContext.sparkContext.getConf)
    val cluster = MemSQLCluster(conf)
    val tableIdent = DefaultSource.getTableIdentifier(parameters)
    val saveConf = SaveToMemSQLConf(conf, Some(mode), parameters)

    val relation = MemSQLTableRelation(cluster, tableIdent, sqlContext)
    relation.insert(data, saveConf)

    relation
  }
}

object DefaultSource {
  def getTableIdentifier(parameters: Map[String, String]): TableIdentifier = {
    val path = parameters.get("path")
    if (path.isEmpty) {
      throw new UnsupportedOperationException("Must specify a path when saving or loading a MemSQL DataSource")
    }
    val sparkTableIdent = CatalystSqlParser.parseTableIdentifier(path.get)
    TableIdentifier(sparkTableIdent.table, sparkTableIdent.database)
  }
}