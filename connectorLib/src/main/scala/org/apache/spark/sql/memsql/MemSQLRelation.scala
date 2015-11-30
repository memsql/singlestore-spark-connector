package org.apache.spark.sql.memsql

import com.memsql.spark.connector.rdd.MemSQLRDD
import com.memsql.spark.connector.util.MemSQLConnectionInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

case class MemSQLRelation(connectionInfo: MemSQLConnectionInfo,
                          query: String,
                          queryParams: Seq[Object],
                          schema: StructType,
                          rdd: RDD[Row],
                          sqlContext: SQLContext) extends BaseRelation with TableScan {

  var output: Seq[Attribute] = null
  val buildScan = rdd
}

object MemSQLRelationUtils {
  def buildDataFrame(sqlContext: SQLContext,
                     cxnInfo: MemSQLConnectionInfo,
                     rdd: MemSQLRDD[Row],
                     schema: StructType): DataFrame = {

    val memsqlRelation = MemSQLRelation(
        cxnInfo, rdd.sql, rdd.sqlParams,
        schema, rdd, sqlContext)

    // We need to let LogicalRelation build the output, and then
    // keep track of the correct output rather than building it on
    // demand later. This is to ensure that exprId's are correct
    // when we build the MemSQLPhysicalRDD.
    val relation = LogicalRelation(memsqlRelation)
    memsqlRelation.output = relation.output

    DataFrame(sqlContext, relation)
  }

  def unapply(l: LogicalRelation): Option[MemSQLRelation] = l match {
    case LogicalRelation(r: TableScan) => r match {
      case r: MemSQLRelation => Some(r)
      case _ => None
    }
  }
}
