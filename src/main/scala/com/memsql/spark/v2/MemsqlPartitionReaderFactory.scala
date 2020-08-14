package com.memsql.spark.v2

import java.sql.{Connection, PreparedStatement}

import com.memsql.spark.SQLGen.VariableList
import com.memsql.spark.{JdbcHelpers, LazyLogging, MemsqlOptions}
import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types.StructType

class MemsqlPartitionReaderFactory(query: String,
                                   variables: VariableList,
                                   options: MemsqlOptions,
                                   schema: StructType)
    extends PartitionReaderFactory
    with LazyLogging {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val conn =
      JdbcUtils.createConnectionFactory(
        JdbcHelpers.getDDLJDBCOptions(options)
      )()
    val stmt = conn.prepareStatement(query)
    JdbcHelpers.fillStatement(stmt, variables)
    val rs = stmt.executeQuery()

    val rows = JdbcUtils.resultSetToRows(rs, schema)

    new SimplePartitionReader(conn, stmt, rows, schema)
  }
}

class SimplePartitionReader(conn: Connection,
                            stmt: PreparedStatement,
                            rows: Iterator[Row],
                            schema: StructType)
    extends PartitionReader[InternalRow] {

  val fromRow = RowEncoder(schema).resolveAndBind().createSerializer()

  def next = rows.hasNext

  override def get(): InternalRow = fromRow(rows.next())

  def close() = {
    stmt.close()
    conn.close()
  }

}
