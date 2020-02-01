package com.memsql.spark

import java.sql.Types

import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types.{BooleanType, DataType, LongType, MetadataBuilder}

case object MemsqlDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:memsql")

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case BooleanType => Option(JdbcType("BOOL", java.sql.Types.BOOLEAN))
    case t           => JdbcUtils.getCommonJDBCType(t)
  }

  override def getCatalystType(sqlType: Int,
                               typeName: String,
                               size: Int,
                               md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.VARBINARY && typeName.equals("BIT") && size != 1) {
      // This could instead be a BinaryType if we'd rather return bit-vectors of up to 64 bits as
      // byte arrays instead of longs.
      md.putLong("binarylong", 1)
      Option(LongType)
    } else if (sqlType == Types.BIT && typeName.equals("TINYINT")) {
      Option(BooleanType)
    } else None
  }

  override def quoteIdentifier(colName: String): String = {
    s"`$colName`"
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)
}
