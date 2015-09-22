package com.memsql.spark.connector.dataframe

/**
 * Representation for a key in MemSQL.
 */
abstract class MemSQLKey {
  def toSQL: String
  def canBeUsedAsShardKey: Boolean = false
}

/**
 * `SHARD KEY`
 * @param columns Columns in this key.
 */
case class Shard(columns: Array[String]) extends MemSQLKey {
  override def toSQL: String = "SHARD(" + columns.map("`" + _ + "`").mkString(",") + ")"
  override def canBeUsedAsShardKey: Boolean = true
}

/**
 * `KEY`
 * @param columns Columns in this key.
 */
case class Key(columns: Array[String]) extends MemSQLKey {
  override def toSQL: String = "KEY(" + columns.map("`" + _ + "`").mkString(",") + ")"
}

/**
 * `KEY USING CLUSTERED COLUMNSTORE`
 * @param columns Columns in this key.
 */
case class KeyUsingClusteredColumnStore(columns: Array[String]) extends MemSQLKey {
  override def toSQL: String = "KEY(" + columns.map("`" + _ + "`").mkString(",") + ") USING CLUSTERED COLUMNSTORE"
}

/**
 * `PRIMARY KEY`
 * @param columns Columns in this key.
 */
case class PrimaryKey(columns: Array[String]) extends MemSQLKey {
  override def toSQL: String = "PRIMARY KEY(" + columns.map("`" + _ + "`").mkString(",") + ")"
  override def canBeUsedAsShardKey: Boolean = true
}

/**
 * `UNIQUE KEY`
 * @param columns Columns in this key.
 */
case class UniqueKey(columns: Array[String]) extends MemSQLKey {
  override def toSQL: String = "UNIQUE KEY(" + columns.map("`" + _ + "`").mkString(",") + ")"
}

object Shard {
  def apply(columns: String*) : MemSQLKey = Shard(columns.toArray)
}

object Key {
  def apply(columns: String*) : MemSQLKey = Key(columns.toArray)
}

object KeyUsingClusteredColumnStore {
  def apply(columns: String*) : MemSQLKey = KeyUsingClusteredColumnStore(columns.toArray)
}

object PrimaryKey {
  def apply(columns: String*) : MemSQLKey = PrimaryKey(columns.toArray)
}

object UniqueKey {
  def apply(columns: String*) : MemSQLKey = UniqueKey(columns.toArray)
}
