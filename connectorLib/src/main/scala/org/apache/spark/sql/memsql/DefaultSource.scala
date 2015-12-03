package org.apache.spark.sql.memsql

import com.memsql.spark.connector.sql.TableIdentifier
import com.memsql.spark.connector.{MemSQLCluster, MemSQLConf}
import org.apache.spark.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, CreatableRelationProvider}

class DefaultSource extends RelationProvider
                    with CreatableRelationProvider
                    with Logging {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val conf = MemSQLConf(sqlContext.sparkContext.getConf)
    val cluster = MemSQLCluster(conf)
    val tableIdent = DefaultSource.getTableIdentifier(parameters)

    MemSQLRelation(cluster, tableIdent, sqlContext)
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

    val relation = MemSQLRelation(cluster, tableIdent, sqlContext)
    relation.insert(data, saveConf)

    relation
  }
}

object DefaultSource {
  def getTableIdentifier(parameters: Map[String, String]): TableIdentifier = {
    val tableName = parameters.get("table")
    if (tableName.isEmpty) {
      throw new UnsupportedOperationException("Must specify a table name when loading a MemSQL DataSource")
    }
    TableIdentifier(tableName.get, parameters.get("database"))
  }
}
