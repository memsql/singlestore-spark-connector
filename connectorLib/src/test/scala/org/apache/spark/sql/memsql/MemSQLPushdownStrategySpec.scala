package org.apache.spark.sql.memsql

import com.memsql.spark.connector.sql.TableIdentifier
import com.memsql.spark.pushdown.MemSQLPushdownStrategy
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.memsql.test.SharedMemSQLContext
import org.scalatest.FlatSpec

class MemSQLPushdownStrategySpec extends FlatSpec with SharedMemSQLContext {

  "MemSQLPushdownStrategy" should "output a plan" in {
    val strategy = new MemSQLPushdownStrategy(sc)

    val cluster = msc.getMemSQLCluster
    val tableIdent = TableIdentifier(msc.getDatabase, "foo")

    cluster.createTable(tableIdent, Nil)
    val memSQLRelation = MemSQLTableRelation(cluster, tableIdent, msc)

    val logicalPlan = LogicalRelation(memSQLRelation)
    val physicalPlan: Seq[SparkPlan] = strategy(logicalPlan)

    assert(!physicalPlan.isEmpty)
  }

}
