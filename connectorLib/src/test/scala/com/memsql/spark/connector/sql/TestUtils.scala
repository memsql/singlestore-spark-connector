// scalastyle:off magic.number file.size.limit regex

package com.memsql.spark.connector.sql

import com.memsql.spark.connector._
import com.memsql.spark.connector.util.JDBCImplicits._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode}

object TestUtils {
  def setupBasic(test: TestBase): Unit = {
    test.withConnection(conn => {
      conn.withStatement(stmt => {
        stmt.execute("""
          CREATE TABLE t
          (id INT PRIMARY KEY, data VARCHAR(200), key(data))
        """)
        stmt.execute("""
          CREATE TABLE s
          (id INT, data VARCHAR(200), key(id), key(data), shard())
        """)
        stmt.execute("""
          CREATE /*!90618 reference */ TABLE r
          (id INT PRIMARY KEY, data VARCHAR(200), key(data))
        """)

        val insertValues = Range(0, 1000)
          .map(i => s"""($i, 'test_data_${"%04d".format(i)}')""")
          .mkString(",")

        stmt.execute("INSERT INTO t VALUES" + insertValues)
        stmt.execute("INSERT INTO s VALUES" + insertValues)
        stmt.execute("INSERT INTO r VALUES" + insertValues)
      })
    })
  }

  def setupAllMemSQLTypes(test: TestBase, types: Seq[TestData.MemSQLType]): String = {
    val tbName = "alltypes"
    val tableDefn = MemSQLTable(TableIdentifier(tbName), types.map(_.columnDefn), Nil)

    test.withStatement(stmt => {
      stmt.execute(s"DROP TABLE IF EXISTS $tbName")
      stmt.execute(tableDefn.toSQL)
    })

    val table = test.ss.table(tbName)
    val numRows = types(0).sampleValues.length
    val values = Range(0, numRows).map(i => {
      Row.fromSeq(types.map(_.sampleValues(i)))
    })
    val nullRow = Seq(Row.fromSeq(types.map(_ => null)))
    val rdd = test.sc.parallelize(values ++ nullRow)

    val df = test.ss.createDataFrame(rdd, table.schema)
    df.saveToMemSQL(tbName)

    tbName
  }

  def collectAndSort(df: DataFrame, asString: Boolean = false, convertBooleans: Boolean = false): Seq[Row] = {
    val rdd = if (asString) {
      df.rdd.map((r: Row) => Row.fromSeq(r.toSeq.map { x =>
        (convertBooleans, x) match {
          case (true, bool: Boolean) => if (bool) "1" else "0"
          case (_, bytes: Array[Byte]) => bytes.toList.toString
          case default => x.toString
        }
      }))
    } else {
      df.rdd.map((r: Row) => Row.fromSeq(r.toSeq.map {
        // byte arrays cannot be compared with equals, so we convert them to lists for comparison purposes
        case bytes: Array[Byte] => bytes.toList
        case default => default
      }))
    }

    rdd.collect.sorted(new Ordering[Row] {
      def stringify(x: Any): String = {
        x match {
          case null => "null"
          case default => x.toString
        }
      }

      override def compare(row1: Row, row2: Row): Int = {
        if (row1 == null) assert(false)
        if (row2 == null) assert(false)
        row1.toSeq.map(stringify).mkString(", ").compareTo(row2.toSeq.map(stringify).mkString(", "))
      }
    })
  }

  def equalDFs(df1: DataFrame, df2: DataFrame, asString: Boolean = false, convertBooleans: Boolean = false): Boolean = {
    val df1_sorted = collectAndSort(df1, asString, convertBooleans)
    val df2_sorted = collectAndSort(df2, asString, convertBooleans)
    if (df1_sorted.size != df2_sorted.size) {
      println("len df1 = " + df1_sorted.size + ", len df2 = " + df2_sorted.size)
      false
    } else {
      var fail = false
      for (i <- df1_sorted.indices) {
        if (!(df1_sorted(i).toSeq sameElements df2_sorted(i).toSeq)) {
          fail = true
          println("row " + i + " is different.")
          if (df1_sorted(i).size != df2_sorted(i).size) {
            println("row sizes are different, " + df1_sorted(i).size + " vs " + df2_sorted(i).size)
          } else {
            for (r <- 0 until df1_sorted(i).size) {
              if ((df1_sorted(i)(r) == null) != (df2_sorted(i)(r) == null)
                || ((df1_sorted(i)(r) != null) && !df1_sorted(i)(r).equals(df2_sorted(i)(r)))) {
                println("difference in column " + r.toString + " : " + df1_sorted(i)(r) + " vs " + df2_sorted(i)(r))
              }
            }
          }
        }
      }
      !fail
    }
  }

  def detachPartitions(test: TestBase): Unit = {
    test.withConnection(conn => {
      conn.withStatement(stmt => {
        val iter = stmt.executeQuery(s"SHOW PARTITIONS ON `${test.dbName}`").toIterator
        iter.foreach { rs =>
          val host = rs.getString("Host")
          val port = rs.getInt("Port")
          val ordinal = rs.getString("Ordinal")
          conn.withStatement(s =>
            s.execute(s"DETACH PARTITION `${test.dbName}`:$ordinal ON `$host`:$port")
          )
        }
      })
    })
  }

  def runQueries[T](bases: (T, T), handler: T => Seq[DataFrame]): Unit = {
    val leftQueries: Seq[DataFrame] = handler(bases._1)
    val rightQueries: Seq[DataFrame] = handler(bases._2)

    val pairs = leftQueries.zip(rightQueries)

    for (i <- pairs.indices) {
      val left = pairs(i)._1
      val right = pairs(i)._2

      try {
        val leftCount = left.count
        val rightCount = right.count
        assert(leftCount > 0 && rightCount > 0,
               s"Dataframes must be nonempty $i: left had $leftCount rows and right had $rightCount rows")
        assert(equalDFs(left, right))
      } catch {
        case e: Throwable =>
          println(
            s"""
               |--------------------------------------------
               |Exception occurred while running the following
               |queries and comparing results:
               |---
               |
                 |Left: ${left.schema}
               """.stripMargin)
          left.explain
          println(s"\nRight: ${right.schema}\n")
          right.explain
          println("--------------------------------------------")
          throw e
      }
    }
  }

  def getTestSaveConf(saveMode: SaveMode = MemSQLConf.DEFAULT_SAVE_MODE,
            onDuplicateKeySQL: Option[String] = None,
            extraColumns: Seq[ColumnDefinition] = Nil,
            extraKeys: Seq[MemSQLKey] = Nil): SaveToMemSQLConf = {

    SaveToMemSQLConf(
      saveMode,
      MemSQLConf.DEFAULT_CREATE_MODE,
      onDuplicateKeySQL,
      MemSQLConf.DEFAULT_INSERT_BATCH_SIZE,
      MemSQLConf.DEFAULT_LOAD_DATA_COMPRESSION,
      false,
      extraColumns,
      extraKeys,
      false
    )
  }

  def exprToColumn(newExpr: Expression): Column = new Column(newExpr)

}
