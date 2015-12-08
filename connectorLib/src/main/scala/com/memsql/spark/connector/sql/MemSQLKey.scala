package com.memsql.spark.connector.sql

/**
 * Representation for a key in MemSQL.
 */
abstract class MemSQLKey {
  def columns: Seq[MemSQLColumn]
  def toSQL: String

  val canBeUsedAsShardKey: Boolean = false
  def columnsSQL: String = columns.map(_.quotedName).mkString(",")

  def columnNames: Seq[String] = columns.map(_.name)
}

/**
 * `SHARD KEY`
 * @param columns Columns in this key.
 */
case class Shard(columns: Seq[MemSQLColumn]=Nil) extends MemSQLKey {
  override def toSQL: String = s"SHARD($columnsSQL)"
  override val canBeUsedAsShardKey: Boolean = true
}

/**
 * `KEY`
 * @param columns Columns in this key.
 */
case class Key(columns: Seq[MemSQLColumn]) extends MemSQLKey {
  override def toSQL: String = s"KEY($columnsSQL)"
}

/**
 * `KEY USING CLUSTERED COLUMNSTORE`
 * @param columns Columns in this key.
 */
case class KeyUsingClusteredColumnStore(columns: Seq[MemSQLColumn]) extends MemSQLKey {
  override def toSQL: String = s"KEY($columnsSQL) USING CLUSTERED COLUMNSTORE"
}

/**
 * `PRIMARY KEY`
 * @param columns Columns in this key.
 */
case class PrimaryKey(columns: Seq[MemSQLColumn]) extends MemSQLKey {
  override def toSQL: String = s"PRIMARY KEY($columnsSQL)"
  override val canBeUsedAsShardKey: Boolean = true
}

/**
 * `UNIQUE KEY`
 * @param columns Columns in this key.
 */
case class UniqueKey(columns: Seq[MemSQLColumn]) extends MemSQLKey {
  override def toSQL: String = s"UNIQUE KEY($columnsSQL)"
}
