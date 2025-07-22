package com.singlestore.spark

import com.singlestore.spark.SQLGen.{Relation, SQLGenContext, VariableList}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

object VersionSpecificRelationExtractor {
  def unapply(source: LogicalPlan): Option[Relation] =
    source match {
      case LogicalRelation(reader: SinglestoreReader,
      output,
      catalogTable,
      isStreaming,
      stream) => {
        def convertBack(output: Seq[AttributeReference],
                        sql: String,
                        variables: VariableList,
                        isFinal: Boolean,
                        context: SQLGenContext): LogicalPlan = {
          new LogicalRelation(
            reader.copy(query = sql,
              variables = variables,
              isFinal = isFinal,
              expectedOutput = output,
              context = context),
            output,
            catalogTable,
            isStreaming,
            stream
          )
        }

        Some(Relation(output, reader, reader.context.nextAlias(), convertBack))
      }
      case _ => None
    }
}
