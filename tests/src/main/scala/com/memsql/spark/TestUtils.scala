// scalastyle:off magic.number file.size.limit regex

package com.memsql.spark

import com.memsql.spark.connector.util.JDBCImplicits._
import org.apache.spark.sql.{Row, DataFrame}

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

  def setupAllMemSQLTypes(test: TestBase, nullable: Boolean, types: Seq[TestData.MemSQLType]): Unit = {
    test.withConnection(conn => {
      conn.withStatement(stmt => {
        val (tbName, nullability) = if (nullable) {
          ("alltypes_nullable", "NULL DEFAULT NULL")
        } else {
          ("alltypes_not_null", "NOT NULL")
        }

        stmt.execute(s"DROP TABLE IF EXISTS $tbName")

        val columns = types
          .map(t => s"${t.name} $nullability")
          .mkString(", ")

        stmt.execute(s"""
          CREATE TABLE $tbName (
            $columns, shard()
          )
        """)

        // all types must have the same number of sampleValues
        val numSampleValues = types(0).sampleValues.length

        val insertValues = Range(0, numSampleValues)
          .map(i => {
            val values = types.map(t => t.sampleValues(i))
                 .map(s => s"'$s'")
                 .mkString(",")
            s"($values)"
          })
          .mkString(",")

        stmt.execute(s"INSERT INTO $tbName VALUES $insertValues")

        if (nullable) {
          // insert an entire row of nulls if the table is nullable
          val values = types.map(_ => "NULL").mkString(",")
          stmt.execute(s"INSERT INTO $tbName VALUES ($values)")
        }
      })
    })
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
        if (!df1_sorted(i).equals(df2_sorted(i))) {
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

    pairs.foreach({
      case (left: DataFrame, right: DataFrame) => {
        try {
          val leftCount = left.count
          val rightCount = right.count
          assert(leftCount > 0 && rightCount > 0,
                 s"Dataframes must be nonempty: left had $leftCount rows and right had $rightCount rows")
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
    })
  }
}
