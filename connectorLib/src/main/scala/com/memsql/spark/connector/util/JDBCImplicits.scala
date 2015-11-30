package com.memsql.spark.connector.util

import java.sql._

object JDBCImplicits {
  implicit class ResultSetHelpers(val rs: ResultSet) {
    def toIterator: Iterator[ResultSet] = new Iterator[ResultSet] {
      def hasNext = rs.next()
      def next() = rs
    }

    def hasColumn(columnName: String): Boolean = {
      val metadata = rs.getMetaData
      Range(0, metadata.getColumnCount)
        .exists(i => columnName == metadata.getColumnName(i + 1))
    }
  }

  implicit class PreparedStatementHelpers(val stmt: PreparedStatement) {
    def fillParams(sqlParams: Seq[Any]): Statement = {
      sqlParams.zipWithIndex.foreach {
        case (el, i) => {
          if (el == null) {
            stmt.setNull(i + 1, Types.NULL)
          } else {
            stmt.setObject(i + 1, el)
          }
        }
      }
      stmt
    }
  }

  implicit class ConnectionHelpers(val conn: Connection) {
    def withStatement[T](handle: Statement => T): T =
      Loan[Statement](conn.createStatement).to(handle)

    def withPreparedStatement[T](query: String, handle: PreparedStatement => T): T =
      Loan[PreparedStatement](conn.prepareStatement(query)).to(handle)
  }
}
