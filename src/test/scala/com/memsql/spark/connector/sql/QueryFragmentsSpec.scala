package com.memsql.spark.connector.sql

import org.scalatest.{FlatSpec, Matchers}

class QueryFragmentsSpec extends FlatSpec with Matchers {

  def tableFragment(tableName: String, dbName: Option[String], columns: Seq[String]): QueryFragment = {
    val ident = TableIdentifier(tableName, dbName)
    QueryFragments.tableNameWithColumns(ident, columns.map(ColumnReference(_)))
  }

  "QueryFragments" should "handle TableIdentifiers with columns" in {
    assert(tableFragment("foo", None, Nil).toSQL == "`foo` ()")
    assert(tableFragment("foo", None, Seq("test1")).toSQL == "`foo` (`test1`)")
    assert(tableFragment("foo 213 #$(*(*%", None, Seq("test1")).toSQL == "`foo 213 #$(*(*%` (`test1`)")
    assert(tableFragment("foo", None, Seq("test1", "test2")).toSQL == "`foo` (`test1`,`test2`)")

    // Note - we want to check that the database name doesn't show up
    assert(tableFragment("foo", Some("bar"), Seq("test1")).toSQL == "`foo` (`test1`)")
    assert(tableFragment("foo", Some("bar"), Seq("test1", "test2")).toSQL == "`foo` (`test1`,`test2`)")
  }

  it should "build LOAD DATA queries" in {
    var q = QueryFragments.loadDataQuery(tableFragment("foo", None, Nil), "gz", "")
    assert(q.toSQL == "LOAD DATA LOCAL INFILE '###.gz'  INTO TABLE `foo` ()")

    q = QueryFragments.loadDataQuery(tableFragment("foo", None, Nil), "txt", "REPLACE")
    assert(q.toSQL == "LOAD DATA LOCAL INFILE '###.txt' REPLACE INTO TABLE `foo` ()")

    q = QueryFragments.loadDataQuery(tableFragment("foo", Some("bar"), Seq("test1")), "txt", "REPLACE")
    assert(q.toSQL == "LOAD DATA LOCAL INFILE '###.txt' REPLACE INTO TABLE `foo` (`test1`)")
  }

  it should "build CREATE DATABASE queries" in {
    var q = QueryFragments.createDatabaseQuery("test", false)
    assert(q.toSQL == "CREATE DATABASE `test`")

    q = QueryFragments.createDatabaseQuery("test", true)
    assert(q.toSQL == "CREATE DATABASE IF NOT EXISTS `test`")
  }
}
