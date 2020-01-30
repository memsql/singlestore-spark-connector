package com.memsql.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

class SQLPushdownRule extends Rule[LogicalPlan] {
  override def apply(root: LogicalPlan): LogicalPlan = {
    val needsPushdown = root
      .find({
        case SQLGen.Relation(r: SQLGen.Relation) if !r.reader.isFinal => true
        case _                                                        => false
      })
      .isDefined

    if (!needsPushdown) {
      return root
    }

    log.trace(s"Optimizing plan:\n${root.treeString(true)}")

    val initializeRelations: PartialFunction[LogicalPlan, LogicalPlan] = {
      case SQLGen.Relation(relation: SQLGen.Relation) => relation.renameOutput
    }
    val finalizeRelations: PartialFunction[LogicalPlan, LogicalPlan] = {
      case SQLGen.Relation(relation: SQLGen.Relation) => relation.castOutputAndFinalize
    }

    val transforms =
      List(
        SQLGen.fromLogicalPlan.andThen(_.asLogicalPlan()),
        SQLGen.fromNestedLogicalPlan.andThen(_.asLogicalPlan()),
        SQLGen.fromSingleLimitSort.andThen(_.asLogicalPlan())
      )

    var ptr, nextPtr = root.transform(initializeRelations)

    do {
      ptr = nextPtr
      nextPtr = transforms.foldLeft(ptr)(_.transformUp(_))
    } while (!ptr.fastEquals(nextPtr))

    val out = ptr.transform(finalizeRelations)

    log.trace(s"Optimized Plan:\n${out.treeString(true)}")
    out
  }
}

object SQLPushdownRule {
  def injected(session: SparkSession): Boolean = {
    session.experimental.extraOptimizations
      .exists(s => s.isInstanceOf[SQLPushdownRule])
  }

  def ensureInjected(session: SparkSession): Unit = {
    if (!injected(session)) {
      session.experimental.extraOptimizations ++= Seq(new SQLPushdownRule)
    }
  }

  def ensureRemoved(session: SparkSession): Unit = {
    session.experimental.extraOptimizations = session.experimental.extraOptimizations
      .filterNot(s => s.isInstanceOf[SQLPushdownRule])
  }
}
