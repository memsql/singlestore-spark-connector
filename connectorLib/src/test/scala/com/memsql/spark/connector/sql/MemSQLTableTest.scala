package com.memsql.spark.connector.sql

import org.scalatest.FlatSpec

class MemSQLTableTest extends FlatSpec {
  "MemSQLTable" should "handle different qualifications of TableIdentifier" in {
    var ident = TableIdentifier("foo")
    var table = MemSQLTable(ident, Seq(ColumnDefinition("foo", "int")), Nil).toSQL
    assert(table == "CREATE TABLE  `foo` (`foo` int NULL DEFAULT NULL, SHARD())")

    ident = TableIdentifier("foo", Some("bar"))
    table = MemSQLTable(ident, Seq(ColumnDefinition("foo", "int")), Nil).toSQL
    assert(table == "CREATE TABLE  `bar`.`foo` (`foo` int NULL DEFAULT NULL, SHARD())")
  }

  it should "handle different numbers of columns" in {
    val ident = TableIdentifier("foo")

    // 2 columns
    var table = MemSQLTable(ident, Seq(ColumnDefinition("foo", "int"), ColumnDefinition("bar", "int")), Nil).toSQL
    assert(table == "CREATE TABLE  `foo` (`foo` int NULL DEFAULT NULL, `bar` int NULL DEFAULT NULL, SHARD())")

    // 1 columns
    table = MemSQLTable(ident, Seq(ColumnDefinition("foo", "int")), Nil).toSQL
    assert(table == "CREATE TABLE  `foo` (`foo` int NULL DEFAULT NULL, SHARD())")

    // 0 columns
    intercept[IllegalArgumentException] {
      MemSQLTable(ident, Nil, Nil).toSQL
    }
  }

  it should "handle different numbers of keys" in {
    val cols = Seq(ColumnDefinition("foo", "int"))
    val ident = TableIdentifier("foo")

    // 2 keys
    // should not inject a shard key since primary key counts as a shard key
    var table = MemSQLTable(ident, cols, Seq(Key("hi"), PrimaryKey("asdf"))).toSQL
    assert(table == "CREATE TABLE  `foo` (`foo` int NULL DEFAULT NULL, KEY(`hi`), PRIMARY KEY(`asdf`))")

    // 2 keys
    // should inject a shard key since unique key doesn't count as one
    table = MemSQLTable(ident, cols, Seq(Key("hi"), UniqueKey("asdf"))).toSQL
    assert(table == "CREATE TABLE  `foo` (`foo` int NULL DEFAULT NULL, KEY(`hi`), UNIQUE KEY(`asdf`), SHARD())")

    // 1 keys
    table = MemSQLTable(ident, cols, Seq(Key("hi"))).toSQL
    assert(table == "CREATE TABLE  `foo` (`foo` int NULL DEFAULT NULL, KEY(`hi`), SHARD())")

    // 0 keys
    table = MemSQLTable(ident, cols, Nil).toSQL
    assert(table == "CREATE TABLE  `foo` (`foo` int NULL DEFAULT NULL, SHARD())")
  }

  it should "support ifNotExists" in {
    val ident = TableIdentifier("foo")
    val table = MemSQLTable(ident, Seq(ColumnDefinition("foo", "int")), Nil, ifNotExists = true).toSQL
    assert(table == "CREATE TABLE IF NOT EXISTS `foo` (`foo` int NULL DEFAULT NULL, SHARD())")
  }
}
