// scalastyle:off magic.number file.size.limit regex

package com.memsql.spark.connector.sql

import com.memsql.spark.SaveToMemSQLException
import com.memsql.spark.connector.{CompressionType, CreateMode}
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException
import org.apache.spark.sql.SaveMode
import org.scalatest.FlatSpec


/**
  * Read/write MemSQL tables using the Spark DataFrame reader and writer APIs
  */
class TestRelationProviderSpec extends FlatSpec with SharedMemSQLContext{

  "TestRelationProviderSpec" should "test" in {
    TestUtils.setupBasic(this)

    val tableNames = Seq("t", "s", "r")
    val memsqlFormat = "com.memsql.spark.connector"

    for (name <- tableNames) {
      // Verify that we can use the DefaultSource to read from each table
      val table = ss
        .read
        .format(memsqlFormat)
        .options(Map("path" -> (dbName + "." + name)))
        .load()

      assert(table.count > 0)
      assert(table.schema.exists(f => f.name == "data"))

      // Verify that we can use the DefaultSource to write to new & existing tables
      val writer = table.write.format(memsqlFormat)

      // we should support all save modes
      for (mode <- SaveMode.values()) {
        val targetName = s"${name}_${mode.name}"

        // mode -> create new table
        writer.mode(mode).save(targetName)

        // verify the data was sent
        val targetName_df =  ss
          .read
          .format(memsqlFormat)
          .options(Map("path" -> targetName))
          .load()

        assert(TestUtils.equalDFs(table, targetName_df.select("id", "data")))

        // mode -> existing table
        writer.mode(mode).save(targetName)

        // verify the data was sent
        assert(TestUtils.equalDFs(table.unionAll(table), targetName_df.select("id", "data")))
      }

      // Test create modes
      val testCreateDB = s"${dbName}_testcreate"
      val testCreateName = s"$testCreateDB.foo"
      withStatement(stmt => stmt.execute(s"DROP DATABASE IF EXISTS $testCreateDB"))

//      println("hi1")
//      // Make sure we killed the target database
//      val maybeTable =  sqlContext
//        .read
//        .format(memsqlFormat)
//        .options(Map("path" -> testCreateName))
//        .load()
//      println("hi2")
//      assert(Some(maybeTable).isEmpty)

      // Skip and Table modes should complain
      // Note - CreateMode.Table will complain in the driver directly
      // since it attempts to create a table first.
      // This is why we need to catch various exceptions
      for (mode <- Seq(CreateMode.Skip, CreateMode.Table)) {
        try {
          writer.option("createMode", mode.toString).save(testCreateName)
        } catch {
          case e: SaveToMemSQLException => assert(e.exception.getMessage.contains("Unknown database"))
          case e: MySQLSyntaxErrorException => assert(e.getMessage.contains("Unknown database"))
        }
      }

      // DatabaseAndTable should create the database and table
      writer.option("createMode", CreateMode.DatabaseAndTable.toString).save(testCreateName)

      val testCreateTable_df = ss
        .read
        .format(memsqlFormat)
        .options(Map( "path" -> testCreateName))
        .load()

      assert(TestUtils.equalDFs(
        table, testCreateTable_df.select("id", "data")))

      // Other options should not cause exceptions
      writer.options(Map(
        "onDuplicateKeySQL" -> "data = 1",
        "insertBatchSize" -> "10",
        "loadDataCompression" -> CompressionType.Skip.toString,
        "useKeylessShardingOptimization" -> "true"
      )).save(testCreateName)
      assert(TestUtils.equalDFs(
        table.unionAll(table),
        testCreateTable_df.select("id", "data")))
    }
  }
}
