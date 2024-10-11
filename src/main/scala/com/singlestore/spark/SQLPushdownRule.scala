package com.singlestore.spark

import com.singlestore.spark.SQLGen.{ExpressionExtractor, SQLGenContext}
import org.apache.spark.{DataSourceTelemetryHelpers, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation

class SQLPushdownRule(sparkContext: SparkContext)
  extends Rule[LogicalPlan]
    with DataSourceTelemetryHelpers {

  override def apply(root: LogicalPlan): LogicalPlan = {
    var context: SQLGenContext = null
    val needsPushdown = root
      .find({
        case SQLGen.Relation(r: SQLGen.Relation) if !r.reader.isFinal =>
          context = SQLGenContext(root, r.reader.options, sparkContext)
          true
        case _ => false
      })
      .isDefined

    if (!needsPushdown) {
      return root
    }

    if (log.isTraceEnabled) {
      log.trace(s"Optimizing plan:\n${root.treeString(true)}")
    }

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

    // Second, we need to cleanup the LogicalRelation (Warehouse Tables) outputs.
    //
    // In Lines 69 - 71 we create Projections with renamed columns for the every query in the Plan.
    // These Projections are equivalent to 'select `a` as `a#` ...'.
    // If the Warehouse Tables have less than 50-100 Columns that is fine because the final SQL
    // query string is not too long.
    // Per Dell's Data Model and future Customer's, we need to account for cases where Warehouse
    // Tables have more than 50-100 Columns (Dell's queries can get to ~130k characters).
    // Hence, we create a Plan wide set of required columns to use for filtering the columns Projected
    // ONLY for Warehouse Tables (Source Queries) and thus keep the impact to a minimum
    //
    // Note: We cannot change Lines 69 - 71 because then Connector's Parallel Read breaks
    val requiredColumns = normalized.collect({
      case plan if !plan.isInstanceOf[LogicalRelation] => plan.references.map(_.exprId)
    }).flatten.toSet

    val filteredSourceQueries = normalized.transform({
      case LogicalRelation(reader: SinglestoreReader, output, catalogTable, isStreaming) =>
        val filteredOutput = output.filter { a => requiredColumns.contains(a.exprId) }
        new LogicalRelation(
          reader.copy(expectedOutput = filteredOutput),
          filteredOutput,
          catalogTable,
          isStreaming
        )
    })

    // Third, we iterate over the LogicalPlan to find Project plans with no fields or SubQueryAlias.
    // If found, they get removed and we return the LogicalPlan without empty Project plans
    // to continue to the generation of the query.
    //
    // Note: Spark is generating empty Project Plan when the query has count(*).
    val filtredOutEmpty = filteredSourceQueries.transform({
      case Project(Nil, child) => child
      case SubqueryAlias(_, child) => child
    })

    // Fourth, we need to rename the outputs of each SingleStore relation in the tree. This transform is
    // done to ensure that we can handle projections which involve ambiguous column name references.
    var ptr, nextPtr = filtredOutEmpty.transform({
      // In edge cases, if `relation.output` is empty it means that we don't use the referenced
      // Warehouse tables at all. Ex: `select hex(null) as hex_col from reviews`. Hence no need to rename.
      case SQLGen.Relation(relation) if relation.output.nonEmpty => relation.renameOutput
    })

    val expressionExtractor = ExpressionExtractor(context)
    val transforms =
      List(
        // do all rewrites except top-level sort, e.g. Project([a,b,c], Relation(select * from foo))
        SQLGen.fromLogicalPlan(expressionExtractor).andThen(_.asLogicalPlan()),
        // do rewrites with top-level Sort, e.g. Sort(a, Limit(10, Relation(select * from foo))
        // won't be done for relations with parallel read enabled
        SQLGen.fromTopLevelSort(expressionExtractor),
      )

    // Run our transforms in a loop until the tree converges
    do {
      ptr = nextPtr
      nextPtr = transforms.foldLeft(ptr)(_.transformUp(_))
    } while (!ptr.fastEquals(nextPtr))

    // Finalize all the relations in the tree and perform casts into the expected output datatype for Spark
    val out = ptr.transform({
      case SQLGen.Relation(relation) if !relation.isFinal => relation.castOutputAndFinalize
    })

    log.info(logEventNameTagger(s"Optimized Plan:\n${out.treeString(true)}"))

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
      session.experimental.extraOptimizations ++= Seq(new SQLPushdownRule(session.sparkContext))
    }
  }

  def ensureRemoved(session: SparkSession): Unit = {
    session.experimental.extraOptimizations = session.experimental.extraOptimizations
      .filterNot(s => s.isInstanceOf[SQLPushdownRule])
  }
}
