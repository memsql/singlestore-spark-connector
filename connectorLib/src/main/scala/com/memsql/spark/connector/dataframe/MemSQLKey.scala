package com.memsql.spark.connector.dataframe

abstract class MemSQLKey
{
    def toSQL: String
    def canBeUsedAsShardKey: Boolean = false
}


case class Shard(columns: Array[String]) extends MemSQLKey
{
    override def toSQL: String = "SHARD(" + columns.mkString(",") + ")"
    override def canBeUsedAsShardKey = true
}
case class Key(columns: Array[String]) extends MemSQLKey
{
    override def toSQL: String = "KEY(" + columns.mkString(",") + ")"
}
case class KeyUsingClusteredColumnStore(columns: Array[String]) extends MemSQLKey
{
    override def toSQL: String = "KEY(" + columns.mkString(",") + ") USING CLUSTERED COLUMNSTORE"
}
case class PrimaryKey(columns: Array[String]) extends MemSQLKey
{
    override def toSQL: String = "PRIMARY KEY(" + columns.mkString(",") + ")"
    override def canBeUsedAsShardKey = true
}
case class UniqueKey(columns: Array[String]) extends MemSQLKey
{
    override def toSQL: String = "UNIQUE KEY(" + columns.mkString(",") + ")"
}

object Shard
{
    def apply(columns: String*) : MemSQLKey = Shard(columns.toArray.map("`" + _ + "`"))
}
object Key
{
    def apply(columns: String*) : MemSQLKey = Key(columns.toArray.map("`" + _ + "`"))
}
object KeyUsingClusteredColumnStore
{
    def apply(columns: String*) : MemSQLKey = KeyUsingClusteredColumnStore(columns.toArray.map("`" + _ + "`"))
}
object PrimaryKey
{
    def apply(columns: String*) : MemSQLKey = PrimaryKey(columns.toArray.map("`" + _ + "`"))
}
object UniqueKey
{
    def apply(columns: String*) : MemSQLKey = UniqueKey(columns.toArray.map("`" + _ + "`"))
}
