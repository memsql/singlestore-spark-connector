package com.memsql.spark.connector.sql

import org.apache.spark.sql.{Row, SaveMode}
import org.scalatest.FlatSpec

class InsertQuerySpec extends FlatSpec {
  val tableFrag = QueryFragments.tableNameWithColumns(TableIdentifier("foo"), Seq(ColumnReference("test")))

  "InsertQuery" should "handle all SaveModes" in {
    val rows = List(Row(1,2))

    var query = new InsertQuery(tableFrag, SaveMode.Append).buildQuery(rows)
    assert(query == s"INSERT IGNORE INTO ${tableFrag.toSQL} VALUES (?,?)")

    query = new InsertQuery(tableFrag, SaveMode.Ignore).buildQuery(rows)
    assert(query == s"INSERT IGNORE INTO ${tableFrag.toSQL} VALUES (?,?)")

    query = new InsertQuery(tableFrag, SaveMode.Overwrite).buildQuery(rows)
    assert(query == s"REPLACE INTO ${tableFrag.toSQL} VALUES (?,?)")

    query = new InsertQuery(tableFrag, SaveMode.ErrorIfExists).buildQuery(rows)
    assert(query == s"INSERT INTO ${tableFrag.toSQL} VALUES (?,?)")
  }

  it should "handle duplicate key sql" in {
    val rows = List(Row(1,2))

    var query = new InsertQuery(tableFrag, SaveMode.Append, Some("FOO")).buildQuery(rows)
    assert(query.startsWith("INSERT INTO"))
    assert(query.endsWith(" ON DUPLICATE KEY UPDATE FOO"))

    query = new InsertQuery(tableFrag, SaveMode.Ignore, Some("bar = 1")).buildQuery(rows)
    assert(query.startsWith("INSERT INTO"))
    assert(query.endsWith(" ON DUPLICATE KEY UPDATE bar = 1"))

    query = new InsertQuery(tableFrag, SaveMode.Overwrite, Some("bar = 1")).buildQuery(rows)
    assert(query.startsWith("INSERT INTO"))

    query = new InsertQuery(tableFrag, SaveMode.ErrorIfExists, Some("bar = 1")).buildQuery(rows)
    assert(query.startsWith("INSERT INTO"))
  }

  it should "handle different row counts" in {
    var rows = List(Row(1))
    var query = new InsertQuery(tableFrag, SaveMode.Append).buildQuery(rows)
    assert(query.contains(" VALUES (?)"))

    rows = List(Row(1, 2))
    query = new InsertQuery(tableFrag, SaveMode.Append).buildQuery(rows)
    assert(query.contains(" VALUES (?,?)"))

    rows = List(Row("asdf", "asdf2"))
    query = new InsertQuery(tableFrag, SaveMode.Append).buildQuery(rows)
    assert(query.contains(" VALUES (?,?)"))

    rows = List(Row("asdf"), Row("test"))
    query = new InsertQuery(tableFrag, SaveMode.Append).buildQuery(rows)
    assert(query.contains(" VALUES (?),(?)"))

    rows = List(Row("asdf", 2), Row("test", 2))
    query = new InsertQuery(tableFrag, SaveMode.Append).buildQuery(rows)
    assert(query.contains(" VALUES (?,?),(?,?)"))

    intercept[IllegalArgumentException] {
      new InsertQuery(tableFrag, SaveMode.Append, Some("FOO")).buildQuery(Nil)
    }
    intercept[IllegalArgumentException] {
      new InsertQuery(tableFrag, SaveMode.Append, Some("FOO")).buildQuery(List(Row()))
    }
    intercept[IllegalArgumentException] {
      new InsertQuery(tableFrag, SaveMode.Append, Some("FOO")).buildQuery(List(Row(1), Row(2,3)))
    }
  }
}
