package com.memsql.spark.v2

import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{
  CatalogDatabase,
  CatalogFunction,
  CatalogStatistics,
  CatalogTable,
  CatalogTablePartition,
  ExternalCatalog
}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.StructType

class MemsqlCatalog extends ExternalCatalog {

  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = ???

  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = ???

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = ???

  override def getDatabase(db: String): CatalogDatabase = ???

  override def databaseExists(db: String): Boolean = {
    println("something")
    true
  }

  override def listDatabases(): Seq[String] = ???

  override def listDatabases(pattern: String): Seq[String] = ???

  override def setCurrentDatabase(db: String): Unit = ???

  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = ???

  override def dropTable(db: String,
                         table: String,
                         ignoreIfNotExists: Boolean,
                         purge: Boolean): Unit = ???

  override def renameTable(db: String, oldName: String, newName: String): Unit = ???

  override def alterTable(tableDefinition: CatalogTable): Unit = ???

  override def alterTableDataSchema(db: String, table: String, newDataSchema: StructType): Unit =
    ???

  override def alterTableStats(db: String, table: String, stats: Option[CatalogStatistics]): Unit =
    ???

  override def getTable(db: String, table: String): CatalogTable = ???

  override def getTablesByName(db: String, tables: Seq[String]): Seq[CatalogTable] = ???

  override def tableExists(db: String, table: String): Boolean = ???

  override def listTables(db: String): Seq[String] = ???

  override def listTables(db: String, pattern: String): Seq[String] = ???

  override def listViews(db: String, pattern: String): Seq[String] = ???

  override def loadTable(db: String,
                         table: String,
                         loadPath: String,
                         isOverwrite: Boolean,
                         isSrcLocal: Boolean): Unit = ???

  override def loadPartition(db: String,
                             table: String,
                             loadPath: String,
                             partition: TablePartitionSpec,
                             isOverwrite: Boolean,
                             inheritTableSpecs: Boolean,
                             isSrcLocal: Boolean): Unit = ???

  override def loadDynamicPartitions(db: String,
                                     table: String,
                                     loadPath: String,
                                     partition: TablePartitionSpec,
                                     replace: Boolean,
                                     numDP: Int): Unit = ???

  override def createPartitions(db: String,
                                table: String,
                                parts: Seq[CatalogTablePartition],
                                ignoreIfExists: Boolean): Unit = ???

  override def dropPartitions(db: String,
                              table: String,
                              parts: Seq[TablePartitionSpec],
                              ignoreIfNotExists: Boolean,
                              purge: Boolean,
                              retainData: Boolean): Unit = ???

  override def renamePartitions(db: String,
                                table: String,
                                specs: Seq[TablePartitionSpec],
                                newSpecs: Seq[TablePartitionSpec]): Unit = ???

  override def alterPartitions(db: String, table: String, parts: Seq[CatalogTablePartition]): Unit =
    ???

  override def getPartition(db: String,
                            table: String,
                            spec: TablePartitionSpec): CatalogTablePartition = ???

  override def getPartitionOption(db: String,
                                  table: String,
                                  spec: TablePartitionSpec): Option[CatalogTablePartition] = ???

  override def listPartitionNames(db: String,
                                  table: String,
                                  partialSpec: Option[TablePartitionSpec]): Seq[String] = ???

  override def listPartitions(db: String,
                              table: String,
                              partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] =
    ???

  override def listPartitionsByFilter(db: String,
                                      table: String,
                                      predicates: Seq[Expression],
                                      defaultTimeZoneId: String): Seq[CatalogTablePartition] = ???

  override def createFunction(db: String, funcDefinition: CatalogFunction): Unit = ???

  override def dropFunction(db: String, funcName: String): Unit = ???

  override def alterFunction(db: String, funcDefinition: CatalogFunction): Unit = ???

  override def renameFunction(db: String, oldName: String, newName: String): Unit = ???

  override def getFunction(db: String, funcName: String): CatalogFunction = ???

  override def functionExists(db: String, funcName: String): Boolean = ???

  override def listFunctions(db: String, pattern: String): Seq[String] = ???
}
