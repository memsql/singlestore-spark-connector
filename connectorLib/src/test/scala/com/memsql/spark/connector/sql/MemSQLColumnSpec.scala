package com.memsql.spark.connector.sql

import org.scalatest.FlatSpec

class MemSQLColumnSpec extends FlatSpec {
  "ColumnReference" should "escape column names" in {
    assert(ColumnReference("foo").toSQL == "`foo`")
    assert(ColumnReference("bar baz 12342@#$").toSQL == "`bar baz 12342@#$`")
  }

  it should "raise exceptions when invalid" in {
    intercept[IllegalArgumentException] {
      ColumnReference("")
    }
  }

  "ColumnDefinition" should "build basic column definition expressions" in {
    assert(ColumnDefinition("foo", "INT", true).toSQL == "`foo` INT NULL DEFAULT NULL")
    assert(
      ColumnDefinition("bar baz", "DECIMAL(32,30)", true).toSQL ==
      "`bar baz` DECIMAL(32,30) NULL DEFAULT NULL")
    assert(ColumnDefinition("bar", "TEXT", false).toSQL == "`bar` TEXT NOT NULL DEFAULT '0'")

    // we should not add a default "default expression" for timestamp
    assert(ColumnDefinition("bar", "timestamp", false).toSQL == "`bar` timestamp NOT NULL ")
    assert(ColumnDefinition("bar", "timestamp", true).toSQL == "`bar` timestamp NULL ")
  }

  it should "raise exceptions when invalid" in {
    intercept[IllegalArgumentException] {
      ColumnDefinition("", "DECIMAL(32,30)")
    }
    intercept[IllegalArgumentException] {
      ColumnDefinition("asdf", "")
    }
    intercept[IllegalArgumentException] {
      ColumnDefinition("asdf", "int", true, Some("hi"), Some("bye"))
    }
  }

  it should "support a persisted expression" in {
    assert(
      ColumnDefinition("bar", "int", true, persisted=Some("UNIX_TIMESTAMP()")).toSQL ==
      "`bar` AS UNIX_TIMESTAMP() PERSISTED int")
    assert(
      ColumnDefinition("bar", "int", false, persisted=Some("UNIX_TIMESTAMP()")).toSQL ==
        "`bar` AS UNIX_TIMESTAMP() PERSISTED int")
  }

  it should "support a custom default expression" in {
    assert(
      ColumnDefinition("bar", "int", true, defaultSQL=Some("UNIX_TIMESTAMP()")).toSQL ==
        "`bar` int NULL DEFAULT UNIX_TIMESTAMP()")
    assert(
      ColumnDefinition("bar", "int", false, defaultSQL=Some("UNIX_TIMESTAMP()")).toSQL ==
        "`bar` int NOT NULL DEFAULT UNIX_TIMESTAMP()")

    // we should be able to specify a explicit default for timestamp
    assert(
      ColumnDefinition("bar", "timestamp", false, defaultSQL=Some("123")).toSQL ==
        "`bar` timestamp NOT NULL DEFAULT 123")
  }
}
