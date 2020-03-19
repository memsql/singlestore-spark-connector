package com.memsql.spark

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.memsql.spark.SQLGen.VariableList
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types._
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

case class MemsqlRDD(query: String,
                     variables: VariableList,
                     options: MemsqlOptions,
                     schema: StructType,
                     expectedOutput: Seq[Attribute],
                     @transient val sc: SparkContext)
    extends RDD[Row](sc, Nil) {

  override protected def getPartitions: Array[Partition] =
    MemsqlQueryHelpers.GetPartitions(options, query, variables)

  override def compute(rawPartition: Partition, context: TaskContext): Iterator[Row] = {
    var closed                     = false
    var rs: ResultSet              = null
    var stmt: PreparedStatement    = null
    var conn: Connection           = null
    var partition: MemsqlPartition = rawPartition.asInstanceOf[MemsqlPartition]

    def tryClose(name: String, what: AutoCloseable): Unit = {
      try {
        if (what != null) { what.close() }
      } catch {
        case e: Exception => logWarning(s"Exception closing $name", e)
      }
    }

    def close(): Unit = {
      if (closed) { return }
      tryClose("resultset", rs)
      tryClose("statement", stmt)
      tryClose("connection", conn)
      closed = true
    }

    context.addTaskCompletionListener { context =>
      close()
    }

    conn = JdbcUtils.createConnectionFactory(partition.connectionInfo)()
    stmt = conn.prepareStatement(partition.query)
    JdbcHelpers.fillStatement(stmt, partition.variables)
    rs = stmt.executeQuery()

    var rowsIter = JdbcUtils.resultSetToRows(rs, schema)

    if (expectedOutput.nonEmpty) {
      val schemaDatatypes   = schema.map(_.dataType)
      val expectedDatatypes = expectedOutput.map(_.dataType)

      if (schemaDatatypes != expectedDatatypes) {
        val columnEncoders = schemaDatatypes.zip(expectedDatatypes).zipWithIndex.map {
          case ((_: StringType, _: NullType), _)     => ((_: Row) => null)
          case ((_: IntegerType, _: BooleanType), i) => ((r: Row) => r.getInt(i) != 0)
          case ((_: LongType, _: BooleanType), i)    => ((r: Row) => r.getLong(i) != 0)

          case ((l, r), i) => {
            options.assert(l == r, s"MemsqlRDD: unable to encode ${l} into ${r}")
            ((r: Row) => r.get(i))
          }
        }

        rowsIter = rowsIter
          .map(row => Row.fromSeq(columnEncoders.map(_(row))))
      }
    }

    CompletionIterator[Row, Iterator[Row]](new InterruptibleIterator[Row](context, rowsIter), close)
  }

}
