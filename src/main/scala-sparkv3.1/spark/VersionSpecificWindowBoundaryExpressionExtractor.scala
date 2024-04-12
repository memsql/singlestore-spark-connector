package com.singlestore.spark

import com.singlestore.spark.SQLGen.{ExpressionExtractor, SQLGenContext, Statement}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryMinus}

case class VersionSpecificWindowBoundaryExpressionExtractor(
                                                             expressionExtractor: ExpressionExtractor) {
  def unapply(arg: Expression): Option[Statement] = {
    arg match {
      case UnaryMinus(expressionExtractor(child), false) =>
        Some(child + "PRECEDING")
      case _ => None
    }
  }
}
