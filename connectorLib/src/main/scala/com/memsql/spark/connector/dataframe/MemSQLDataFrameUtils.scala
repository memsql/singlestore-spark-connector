package com.memsql.spark.connector.dataframe

import java.sql.{ResultSet, ResultSetMetaData}

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.types._

object MemSQLDataFrameUtils {
  def DataFrameTypeToMemSQLTypeString(dataType: DataType): String = {
    // we match types having _.typeName not a MemSQL type (for instance ShortType.typeName  is "SHORT", but MemSQL calls it "TINYINT")
    dataType match {
      case ShortType => "TINYINT"
      case LongType => "BIGINT"
      case ByteType => "TINYINT"
      case BooleanType => "BOOLEAN"
      case StringType => "TEXT"
      case BinaryType => "BLOB"
      case DecimalType.Unlimited => "DOUBLE"
      case _ => dataType.typeName
    }
  }

  /**
   * Attempts a best effort conversion from a SparkType
   * to a MemSQLType to be used in a Cast.
   *
   * @note Will raise a match error for unsupported casts
   */
  def DataFrameTypeToMemSQLCastType(t: DataType): String = t match {
    case StringType => "CHAR"
    case BinaryType => "BINARY"
    case DateType => "DATE"
    case TimestampType => "DATE"
    case decimal: DecimalType => s"DECIMAL(${decimal.precision}, ${decimal.scale})"
    case BooleanType => "SIGNED INTEGER"
    case ByteType => "CHAR"
    case ShortType => "SIGNED INTEGER"
    case IntegerType => "SIGNED INTEGER"
    case FloatType => "DECIMAL"
    case LongType => "SIGNED"
    case DoubleType => "DECIMAL"
  }

  def JDBCTypeToDataFrameType(rsmd: ResultSetMetaData, ix: Int): DataType = {
    rsmd.getColumnType(ix) match {
      case java.sql.Types.CHAR => StringType
      case java.sql.Types.INTEGER => IntegerType
      case java.sql.Types.TINYINT => ShortType
      case java.sql.Types.SMALLINT => ShortType
      case java.sql.Types.BIGINT => LongType
      case java.sql.Types.DOUBLE => DoubleType
      case java.sql.Types.NUMERIC => DoubleType
      case java.sql.Types.REAL => FloatType
      case java.sql.Types.BIT => BooleanType
      case java.sql.Types.CLOB => StringType
      case java.sql.Types.TIMESTAMP => TimestampType
      case java.sql.Types.DATE => DateType
      case java.sql.Types.TIME => TimestampType
      case java.sql.Types.DECIMAL => DecimalType(
        math.min(DecimalType.MAX_PRECISION, rsmd.getPrecision(ix)),
        math.min(DecimalType.MAX_SCALE, rsmd.getScale(ix))
      )
      case java.sql.Types.LONGNVARCHAR => StringType
      case java.sql.Types.LONGVARCHAR => StringType
      case java.sql.Types.VARCHAR => StringType
      case java.sql.Types.NVARCHAR => StringType
      case java.sql.Types.BLOB => BinaryType
      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.VARBINARY => BinaryType
      case java.sql.Types.BINARY => BinaryType
      case _ => throw new IllegalArgumentException("Can't translate type " + rsmd.getColumnTypeName(ix))
    }
  }

  private def getByteArrayFromBlob(row: ResultSet, ix: Int): Array[Byte] = {
    // get the blob and then check if the read value was null.
    val blob = row.getBlob(ix)
    row.wasNull match {
      case true => null
      case false => IOUtils.toByteArray(blob.getBinaryStream)
    }
  }

  def GetJDBCValue(dataType: Int, ix: Int, row: ResultSet): Any = {
    val result = dataType match {
      case java.sql.Types.CHAR => row.getString(ix)
      case java.sql.Types.INTEGER => row.getInt(ix)
      case java.sql.Types.BIGINT => row.getLong(ix)
      case java.sql.Types.TINYINT => row.getShort(ix)
      case java.sql.Types.SMALLINT => row.getShort(ix)
      case java.sql.Types.DOUBLE => row.getDouble(ix)
      case java.sql.Types.NUMERIC => row.getDouble(ix)
      case java.sql.Types.REAL => row.getFloat(ix)
      case java.sql.Types.BIT => row.getBoolean(ix)
      case java.sql.Types.CLOB => row.getString(ix)
      case java.sql.Types.TIMESTAMP => row.getTimestamp(ix)
      case java.sql.Types.DATE => row.getDate(ix)
      case java.sql.Types.TIME => row.getTime(ix)
      case java.sql.Types.DECIMAL => row.getBigDecimal(ix)
      case java.sql.Types.LONGNVARCHAR => row.getString(ix)
      case java.sql.Types.LONGVARCHAR => row.getString(ix)
      case java.sql.Types.VARCHAR => row.getString(ix)
      case java.sql.Types.NVARCHAR => row.getString(ix)
      // NOTE: java.sql.Blob isn't serializable so we return a byte array instead
      case java.sql.Types.BLOB => getByteArrayFromBlob(row, ix)
      case java.sql.Types.LONGVARBINARY => getByteArrayFromBlob(row, ix)
      case java.sql.Types.VARBINARY => getByteArrayFromBlob(row, ix)
      case java.sql.Types.BINARY => getByteArrayFromBlob(row, ix)
      case _ => throw new IllegalArgumentException("Can't translate type " + dataType.toString)
    }
    if (row.wasNull) null else result
  }
}
