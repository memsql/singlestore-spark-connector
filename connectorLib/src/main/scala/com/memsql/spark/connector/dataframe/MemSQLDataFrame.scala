package com.memsql.spark.connector.dataframe

import java.sql.{PreparedStatement, Connection, ResultSet, ResultSetMetaData, Types}

import com.memsql.spark.connector.util.MemSQLConnectionInfo
import com.memsql.spark.connector.{MemSQLConnectionWrapper, MemSQLConnectionPoolMap}
import com.memsql.spark.context.MemSQLContext
import com.memsql.spark.pushdown.MemSQLPushdownException
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.memsql.MemSQLRelationUtils
import org.apache.spark.{sql, SparkContext}

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import com.memsql.spark.connector.rdd.MemSQLRDD

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
      // Note: Spark's Decimal type only handles: (quote from comment in DecimalType code)
      // "The precision can be up to 38, scale can also be up to 38 (less or equal to precision)."
      case java.sql.Types.DECIMAL => DecimalType(math.min(38, rsmd.getPrecision(ix)), math.min(38, rsmd.getScale(ix)))
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

object MemSQLDataFrame {
  def MakeMemSQLRowRDD(sparkContext: SparkContext,
                       dbHost: String, dbPort: Int, user: String, password: String, dbName: String,
                       query: String): MemSQLRDD[Row] =
    MakeMemSQLRowRDD(
      sparkContext,
      MemSQLConnectionInfo(dbHost, dbPort, user, password, dbName),
      query,
      Nil)

  def MakeMemSQLRowRDD(sparkContext: SparkContext,
                       dbHost: String, dbPort: Int, user: String, password: String, dbName: String,
                       query: String,
                       queryParams: Seq[Object]): MemSQLRDD[Row] =
    MakeMemSQLRowRDD(
      sparkContext,
      MemSQLConnectionInfo(dbHost, dbPort, user, password, dbName),
      query,
      queryParams)

  def MakeMemSQLRowRDD(sparkContext: SparkContext,
                       info: MemSQLConnectionInfo,
                       query: String,
                       queryParams: Seq[Object]=Nil): MemSQLRDD[Row] =
    new MemSQLRDD(
      sparkContext,
      info.dbHost, info.dbPort, info.user, info.password, info.dbName,
      query, queryParams,
      (r: ResultSet) => {
        val count = r.getMetaData.getColumnCount
        Row.fromSeq(Range(0, count)
          .map(i => MemSQLDataFrameUtils.GetJDBCValue(r.getMetaData.getColumnType(i + 1), i + 1, r)))
      }
    )

  def MakeMemSQLDF(sqlContext: SQLContext,
                   dbHost: String, dbPort: Int, user: String, password: String, dbName: String,
                   query: String): DataFrame =
    MakeMemSQLDF(
      sqlContext,
      MemSQLConnectionInfo(dbHost, dbPort, user, password, dbName),
      query)

  /**
   * Make a DataFrame from a MemSQL query.
   *
   * @note If sqlContext is not a MemSQLContext, you will not get any query pushdown.
   * @throws MemSQLPushdownException
   */
  def MakeMemSQLDF(sqlContext: SQLContext, info: MemSQLConnectionInfo,
                   query: String, queryParams: Seq[Object]=Nil): DataFrame = {

    if (sqlContext.isInstanceOf[MemSQLContext]) {
      val m = sqlContext.asInstanceOf[MemSQLContext]
      if (m.pushdownEnabled) {
        // In the case that we are building a DataFrame with a MemSQLContext
        // which has pushdown enabled, we must verify that the connection details
        // correlate to one of the valid aggregators in the cluster.
        val availableNodes = m.getMemSQLNodesAvailableForIngest(alwaysIncludeMaster = true)
        if (!availableNodes.exists(n => info == n)) {
          throw new MemSQLPushdownException(
            "Specified connection info does not refer to a MemSQL node in the associated MemSQLContext's cluster")
        }
      }
    }

    UnsafeMakeMemSQLDF(sqlContext, info, query, queryParams)
  }

  /**
   * Same as [[MakeMemSQLDF]] - but doesn't verify that the query is
   * part of the same cluster as an underlying [[MemSQLContext]]
   */
  def UnsafeMakeMemSQLDF(sqlContext: SQLContext, info: MemSQLConnectionInfo,
                         query: String, queryParams: Seq[Object]=Nil): DataFrame = {
    val rdd = MakeMemSQLRowRDD(sqlContext.sparkContext, info, query, queryParams)
    val schema = getQuerySchema(info, query, queryParams)

    MemSQLRelationUtils.buildDataFrame(sqlContext, info, rdd, schema)
  }

  def getQuerySchema(
    info: MemSQLConnectionInfo,
    query: String,
    queryParams: Seq[Object] = Nil): StructType = {

    var wrapper: MemSQLConnectionWrapper = null
    var conn: Connection = null
    var schemaStmt: PreparedStatement = null
    try {
      wrapper = MemSQLConnectionPoolMap(info)
      conn = wrapper.conn
      schemaStmt = conn.prepareStatement(limitZero(query))
      MemSQLRDD.fillParams(schemaStmt, queryParams)

      val metadata = schemaStmt.executeQuery.getMetaData
      val count = metadata.getColumnCount
      StructType(Range(0,count).map(i => StructField(metadata.getColumnName(i + 1),
                                                     MemSQLDataFrameUtils.JDBCTypeToDataFrameType(metadata, i + 1), true)))
    } finally {
      if (schemaStmt != null && !schemaStmt.isClosed()) {
        schemaStmt.close()
      }
      if (null != conn && !conn.isClosed()) {
        MemSQLConnectionPoolMap.returnConnection(wrapper)
      }
    }
  }

  def limitZero(q: String): String = "SELECT * FROM (" + q + ") tab_alias LIMIT 0"
}
