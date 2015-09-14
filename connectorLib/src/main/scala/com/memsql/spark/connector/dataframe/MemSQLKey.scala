package com.memsql.spark.connector.dataframe

abstract class MemSQLKey
{
    def toSQL: String
    def canBeUsedAsShardKey: Boolean = false
}


case class Shard(columns: Array[String]) extends MemSQLKey
{
    override def toSQL: String = "SHARD(" + columns.map("`" + _ + "`").mkString(",") + ")"
    override def canBeUsedAsShardKey = true
}
case class Key(columns: Array[String]) extends MemSQLKey
{
    override def toSQL: String = "KEY(" + columns.map("`" + _ + "`").mkString(",") + ")"
}
case class KeyUsingClusteredColumnStore(columns: Array[String]) extends MemSQLKey
{
    override def toSQL: String = "KEY(" + columns.map("`" + _ + "`").mkString(",") + ") USING CLUSTERED COLUMNSTORE"
}
case class PrimaryKey(columns: Array[String]) extends MemSQLKey
{
    override def toSQL: String = "PRIMARY KEY(" + columns.map("`" + _ + "`").mkString(",") + ")"
    override def canBeUsedAsShardKey = true
}
case class UniqueKey(columns: Array[String]) extends MemSQLKey
{
    override def toSQL: String = "UNIQUE KEY(" + columns.map("`" + _ + "`").mkString(",") + ")"
}

object Shard
{
    def apply(columns: String*) : MemSQLKey = Shard(columns.toArray)
}
object Key
{
    def apply(columns: String*) : MemSQLKey = Key(columns.toArray)
}
object KeyUsingClusteredColumnStore
{
    def apply(columns: String*) : MemSQLKey = KeyUsingClusteredColumnStore(columns.toArray)
}
object PrimaryKey
{
    def apply(columns: String*) : MemSQLKey = PrimaryKey(columns.toArray)
}
object UniqueKey
{
    def apply(columns: String*) : MemSQLKey = UniqueKey(columns.toArray)
}
