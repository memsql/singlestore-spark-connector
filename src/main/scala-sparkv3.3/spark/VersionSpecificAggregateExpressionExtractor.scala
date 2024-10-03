package com.singlestore.spark

import com.singlestore.spark.SQLGen.{DoubleVar, ExpressionExtractor, SQLGenContext, Statement}
import com.singlestore.spark.ExpressionGen.{aggregateWithFilter, doubleFoldableExtractor, numberFoldableExtractor, f, op}
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateFunction,
  ApproximatePercentile,
  Average,
  First,
  Kurtosis,
  Last,
  Skewness,
  StddevPop,
  StddevSamp,
  Sum,
  VariancePop,
  VarianceSamp
}
import org.apache.spark.sql.types.NumericType

case class VersionSpecificAggregateExpressionExtractor(expressionExtractor: ExpressionExtractor,
                                                       context: SQLGenContext,
                                                       filter: Option[SQLGen.Joinable]) {
  def unapply(aggFunc: AggregateFunction): Option[Statement] = {
    aggFunc match {
      // CentralMomentAgg.scala
      case StddevPop(expressionExtractor(child), true) =>
        Some(aggregateWithFilter("STDDEV_POP", child, filter))
      case StddevSamp(expressionExtractor(child), true) =>
        Some(aggregateWithFilter("STDDEV_SAMP", child, filter))
      case VariancePop(expressionExtractor(child), true) =>
        Some(aggregateWithFilter("VAR_POP", child, filter))
      case VarianceSamp(expressionExtractor(child), true) =>
        Some(aggregateWithFilter("VAR_SAMP", child, filter))
      case Kurtosis(expressionExtractor(child), true) =>
        // ( (AVG(POW(child, 4)) - AVG(child) * POW(AVG(child), 3) * 4  + 6 * AVG(POW(child), 2) * POW(AVG(child), 2)  - 3 * POW(AVG(child), 4) )
        //  / POW(STD(child), 4) )  - 3
        // following the formula from https://stats.oarc.ucla.edu/other/mult-pkg/faq/general/faq-whats-with-the-different-formulas-for-kurtosis/ article
        Some(
          op(
            "-",
            op(
              "/",
              op(
                "-",
                op(
                  "+",
                  op(
                    "-",
                    aggregateWithFilter("AVG", f("POW", child, "4"), filter),
                    op("*",
                       op("*",
                          aggregateWithFilter("AVG", child, filter),
                          aggregateWithFilter("AVG", f("POW", child, "3"), filter)),
                       "4")
                  ),
                  op("*",
                     "6",
                     op("*",
                        aggregateWithFilter("AVG", f("POW", child, "2"), filter),
                        f("POW", aggregateWithFilter("AVG", child, filter), "2")))
                ),
                op("*", "3", f("POW", aggregateWithFilter("AVG", child, filter), "4"))
              ),
              f("POW", aggregateWithFilter("STD", child, filter), "4")
            ),
            "3"
          )
        )

      case Skewness(expressionExtractor(child), true) =>
        // (AVG(POW(child, 3)) - AVG(child) * POW(STD(child), 2) * 3 - POW(AVG(child), 3) ) / POW(STD(child), 3)
        // following the definition section in https://en.wikipedia.org/wiki/Skewness
        Some(
          op(
            "/",
            op(
              "-",
              op(
                "-",
                aggregateWithFilter("AVG", f("POW", child, "3"), filter),
                op("*",
                   op("*",
                      aggregateWithFilter("AVG", child, filter),
                      f("POW", aggregateWithFilter("STD", child, filter), "2")),
                   "3")
              ),
              f("POW", aggregateWithFilter("AVG", child, filter), "3")
            ),
            f("POW", aggregateWithFilter("STD", child, filter), "3")
          )
        )

      // First.scala
      // TODO: case First(expressionExtractor(child), false) => Some(aggregateWithFilter("ANY_VALUE", child, filter))

      // Last.scala
      // TODO: case Last(expressionExtractor(child), false) => Some(aggregateWithFilter("ANY_VALUE", child, filter))

      // Sum.scala
      case Sum(expressionExtractor(child), false) =>
        Some(aggregateWithFilter("SUM", child, filter))

      // Average.scala
      case Average(expressionExtractor(child), false) =>
        Some(aggregateWithFilter("AVG", child, filter))

      // ApproximatePercentile.scala
      case ApproximatePercentile(e @ expressionExtractor(child),
                                 doubleFoldableExtractor(percentage),
                                 numberFoldableExtractor(accuracy), _, _)
        // SingleStore supports columns of numeric data type,
        // percentage only from [0, 1] and error_tolerance (`1.0/accuracy`) from (0,0.5]
        if e.dataType.isInstanceOf[NumericType] &&
            percentage >= 0.0 && percentage <= 1.0 &&
              1.0 / accuracy.longValue() > 0.0 && 1.0 / accuracy.longValue() <= 0.5 =>
        Some(
          aggregateWithFilter(
            "APPROX_PERCENTILE",
            child,
            filter,
            Seq(
              DoubleVar(percentage),
              // SingleStore supports `error_tolerance` which is `1.0/accuracy`
              //
              // Need to pass the calculated value here to avoid the following error:
              // java.sql.SQLException: (conn=<id>) Leaf Error (<ip>): accuracy should be a constant value.
              DoubleVar(1.0 / accuracy.longValue())
            )
          )
        )

      case _ => None
    }
  }
}
