package com.memsql.spark.connector.dataframe

import java.sql.{ResultSet, ResultSetMetaData, Types => JDBCTypes}

import com.google.common.primitives.UnsignedLongs
import org.apache.spark.sql.types._

object TypeConversions {
  val MEMSQL_DECIMAL_MAX_PRECISION = 65
  val MEMSQL_DECIMAL_MAX_SCALE = 30

  def decimalTypeToMySQLType(decimal: DecimalType): String = {
    val precision = Math.min(MEMSQL_DECIMAL_MAX_PRECISION, decimal.precision)
    val scale = Math.min(MEMSQL_DECIMAL_MAX_SCALE, decimal.scale)
    s"DECIMAL($precision, $scale)"
  }

  /**
   * Find the appropriate MemSQL type from a SparkSQL type.
   *
   * Most types share the same name but there are a few special cases because
   * the types don't align perfectly.
   *
   * @param dataType A SparkSQL Type
   * @return Corresponding MemSQL type
   */
  def DataFrameTypeToMemSQLTypeString(dataType: DataType): String = {
    dataType match {
      case ShortType => "SMALLINT"
      case LongType => "BIGINT"
      case BooleanType => "BOOLEAN"
      case StringType => "TEXT"
      case BinaryType => "BLOB"
      case dt: DecimalType => decimalTypeToMySQLType(dt)
      case _ => dataType.typeName
    }
  }

  def JDBCTypeToDataFrameType(rsmd: ResultSetMetaData, ix: Int): DataType = {
    rsmd.getColumnType(ix) match {
      // scalastyle:off

      case JDBCTypes.TINYINT => ShortType
      case JDBCTypes.SMALLINT => ShortType
      case JDBCTypes.INTEGER => if (rsmd.isSigned(ix)) { IntegerType } else { LongType }
      case JDBCTypes.BIGINT => if (rsmd.isSigned(ix)) { LongType } else { DecimalType(20,0) }

      case JDBCTypes.DOUBLE => DoubleType
      case JDBCTypes.NUMERIC => DoubleType
      case JDBCTypes.REAL => FloatType
      case JDBCTypes.DECIMAL => DecimalType(
        math.min(DecimalType.MAX_PRECISION, rsmd.getPrecision(ix)),
        math.min(DecimalType.MAX_SCALE, rsmd.getScale(ix))
      )

      case JDBCTypes.TIMESTAMP => TimestampType
      case JDBCTypes.DATE => DateType
      // MySQL TIME type is represented as a string
      case JDBCTypes.TIME => StringType

      case JDBCTypes.CHAR => StringType
      case JDBCTypes.VARCHAR => StringType
      case JDBCTypes.NVARCHAR => StringType
      case JDBCTypes.LONGVARCHAR => StringType
      case JDBCTypes.LONGNVARCHAR => StringType

      case JDBCTypes.BIT => BinaryType
      case JDBCTypes.BINARY => BinaryType
      case JDBCTypes.VARBINARY => BinaryType
      case JDBCTypes.LONGVARBINARY => BinaryType
      case JDBCTypes.BLOB => BinaryType

      case _ => throw new IllegalArgumentException("Can't translate type " + rsmd.getColumnTypeName(ix))
    }
  }

  def GetJDBCValue(dataType: Int, isSigned: Boolean, ix: Int, row: ResultSet): Any = {
    val result = dataType match {
      case JDBCTypes.TINYINT => row.getShort(ix)
      case JDBCTypes.SMALLINT => row.getShort(ix)
      case JDBCTypes.INTEGER => if (isSigned) { row.getInt(ix).asInstanceOf[Any] } else { row.getLong(ix).asInstanceOf[Any] }
      case JDBCTypes.BIGINT => if (isSigned) {
        row.getLong(ix)
      } else {
        Option(row.getBigDecimal(ix)).map(Decimal(_, 20, 0)).getOrElse(null)
      }

      case JDBCTypes.DOUBLE => row.getDouble(ix)
      case JDBCTypes.NUMERIC => row.getDouble(ix)
      case JDBCTypes.REAL => row.getFloat(ix)
      case JDBCTypes.DECIMAL => row.getBigDecimal(ix)

      case JDBCTypes.TIMESTAMP => row.getTimestamp(ix)
      case JDBCTypes.DATE => row.getDate(ix)

      case JDBCTypes.TIME => Option(row.getTime(ix)).map(_.toString).getOrElse(null)

      case JDBCTypes.CHAR => row.getString(ix)
      case JDBCTypes.VARCHAR => row.getString(ix)
      case JDBCTypes.NVARCHAR => row.getString(ix)
      case JDBCTypes.LONGNVARCHAR => row.getString(ix)
      case JDBCTypes.LONGVARCHAR => row.getString(ix)

      case JDBCTypes.BIT => row.getBytes(ix)
      case JDBCTypes.BINARY => row.getBytes(ix)
      case JDBCTypes.VARBINARY => row.getBytes(ix)
      case JDBCTypes.LONGVARBINARY => row.getBytes(ix)
      case JDBCTypes.BLOB => row.getBytes(ix)

      case _ => throw new IllegalArgumentException("Can't translate type " + dataType.toString)
    }

    if (row.wasNull) null else result
  }
}
