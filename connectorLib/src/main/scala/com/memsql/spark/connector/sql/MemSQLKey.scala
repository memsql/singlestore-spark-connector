package com.memsql.spark.connector.sql

/**
 * Representation for a key in MemSQL.
 */
sealed trait MemSQLKey {
  def columns: Seq[ColumnReference]
  def toSQL: String

  val canBeUsedAsShardKey: Boolean = false
  def columnsSQL: String = columns.map(_.quotedName).mkString(",")
  def columnNames: Seq[String] = columns.map(_.name)
}

/**
 * `SHARD KEY`
 */
case class Shard(columns: Seq[ColumnReference]=Nil) extends MemSQLKey {
  override def toSQL: String = s"SHARD($columnsSQL)"
  override val canBeUsedAsShardKey: Boolean = true
}

object Shard {
  def apply(column: String): Shard = Shard(Seq(ColumnReference(column)))
}

/**
 * `KEY`
 */
case class Key(columns: Seq[ColumnReference]) extends MemSQLKey {
  override def toSQL: String = s"KEY($columnsSQL)"
}

object Key {
  def apply(column: String): Key = Key(Seq(ColumnReference(column)))
}

/**
 * `KEY USING CLUSTERED COLUMNSTORE`
 */
case class KeyUsingClusteredColumnStore(columns: Seq[ColumnReference]) extends MemSQLKey {
  override def toSQL: String = s"KEY($columnsSQL) USING CLUSTERED COLUMNSTORE"
}

object KeyUsingClusteredColumnStore {
  def apply(column: String): KeyUsingClusteredColumnStore =
    KeyUsingClusteredColumnStore(Seq(ColumnReference(column)))
}

/**
 * `PRIMARY KEY`
 */
case class PrimaryKey(columns: Seq[ColumnReference]) extends MemSQLKey {
  override def toSQL: String = s"PRIMARY KEY($columnsSQL)"
  override val canBeUsedAsShardKey: Boolean = true
}

object PrimaryKey {
  def apply(column: String): PrimaryKey = PrimaryKey(Seq(ColumnReference(column)))
}

/**
 * `UNIQUE KEY`
 */
case class UniqueKey(columns: Seq[ColumnReference]) extends MemSQLKey {
  override def toSQL: String = s"UNIQUE KEY($columnsSQL)"
}

object UniqueKey {
  def apply(column: String): UniqueKey = UniqueKey(Seq(ColumnReference(column)))
}
