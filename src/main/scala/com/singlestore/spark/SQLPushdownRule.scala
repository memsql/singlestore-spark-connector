package com.singlestore.spark

import com.singlestore.spark.SQLGen.{ExpressionExtractor, SQLGenContext}
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

    if (log.isTraceEnabled) {
      log.trace(s"Optimizing plan:\n${root.treeString(true)}")
    }

    val context = SQLGenContext(root)

    // We first need to set a SQLGenContext in every reader.
    // This transform is done to ensure that we will generate the same aliases in the same queries.
    val normalized = root.transform({
      case SQLGen.Relation(relation) =>
        relation.toLogicalPlan(
          relation.output,
          relation.reader.query,
          relation.reader.variables,
          relation.reader.isFinal,
          context
        )
    })

    // Second, we need to rename the outputs of each SingleStore relation in the tree.  This transform is
    // done to ensure that we can handle projections which involve ambiguous column name references.
    var ptr, nextPtr = normalized.transform({
      case SQLGen.Relation(relation) => relation.renameOutput
    })

    val expressionExtractor = ExpressionExtractor(context)
    val transforms =
      List(
        // do single node rewrites, e.g. Project([a,b,c], Relation(select * from foo))
        SQLGen.fromLogicalPlan(expressionExtractor).andThen(_.asLogicalPlan()),
        // do multi node rewrites, e.g. Sort(a, Limit(10, Relation(select * from foo)))
        SQLGen.fromNestedLogicalPlan(expressionExtractor).andThen(_.asLogicalPlan()),
        // do single node rewrites of sort & limit (so the multi-node rewrite can match first)
        SQLGen.fromSingleLimitSort(expressionExtractor).andThen(_.asLogicalPlan())
      )

    // Run our transforms in a loop until the tree converges
    do {
      ptr = nextPtr
      nextPtr = transforms.foldLeft(ptr)(_.transformUp(_))
    } while (!ptr.fastEquals(nextPtr))

    // Finalize all the relations in the tree and perform casts into the expected output datatype for Spark
    val out = ptr.transformDown({
      case SQLGen.Relation(relation) if !relation.isFinal => relation.castOutputAndFinalize
    })

    if (log.isTraceEnabled) {
      log.trace(s"Optimized Plan:\n${out.treeString(true)}")
    }

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
