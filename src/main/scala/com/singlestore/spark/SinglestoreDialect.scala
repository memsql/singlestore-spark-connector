package com.singlestore.spark

import java.sql.Types

import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types._

case object SinglestoreDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:memsql")

  val SINGLESTORE_DECIMAL_MAX_SCALE = 30

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case BooleanType   => Option(JdbcType("BOOL", Types.BOOLEAN))
    case ByteType      => Option(JdbcType("TINYINT", Types.TINYINT))
    case ShortType     => Option(JdbcType("SMALLINT", Types.SMALLINT))
    case FloatType     => Option(JdbcType("FLOAT", Types.FLOAT))
    case TimestampType => Option(JdbcType("TIMESTAMP(6)", Types.TIMESTAMP))
    case dt: DecimalType if (dt.scale <= SINGLESTORE_DECIMAL_MAX_SCALE) =>
      Option(JdbcType(s"DECIMAL(${dt.precision}, ${dt.scale})", Types.DECIMAL))
    case dt: DecimalType =>
      throw new IllegalArgumentException(
        s"Too big scale specified(${dt.scale}). SingleStore DECIMAL maximum scale is ${SINGLESTORE_DECIMAL_MAX_SCALE}")
    case NullType =>
      throw new IllegalArgumentException(
        "No corresponding SingleStore type found for NullType. If you want to use NullType, please write to an already existing SingleStore table.")
    case t => JdbcUtils.getCommonJDBCType(t)
  }

  override def getCatalystType(sqlType: Int,
                               typeName: String,
                               size: Int,
                               md: MetadataBuilder): Option[DataType] = {
    (sqlType, typeName) match {
      case (Types.REAL, "FLOAT")        => Option(FloatType)
      case (Types.BIT, "BIT")           => Option(BinaryType)
      // JDBC driver returns incorrect SQL type for BIT
      // TODO delete after PLAT-6829 is fixed
      case (Types.BOOLEAN, "BIT")           => Option(BinaryType)
      case (Types.TINYINT, "TINYINT")   => Option(ShortType)
      case (Types.SMALLINT, "SMALLINT") => Option(ShortType)
      case (Types.INTEGER, "SMALLINT")  => Option(IntegerType)
      case (Types.INTEGER, "SMALLINT UNSIGNED")  => Option(IntegerType)
      case (Types.DECIMAL, "DECIMAL") => {
        if (size > DecimalType.MAX_PRECISION) {
          throw new IllegalArgumentException(
            s"DECIMAL precision ${size} exceeds max precision ${DecimalType.MAX_PRECISION}")
        } else {
          Option(
            DecimalType(size, md.build().getLong("scale").toInt)
          )
        }
      }
      case _ => None
    }
  }

  override def quoteIdentifier(colName: String): String = {
    s"`${colName.replace("`", "``")}`"
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)
}
