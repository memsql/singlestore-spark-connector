package com.singlestore.spark

import com.singlestore.spark.SQLGen.{ExpressionExtractor, SQLGenContext, Statement, StringVar}
import com.singlestore.spark.ExpressionGen.{aggregateWithFilter, f, op}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, Average, First, Kurtosis, Last, Skewness, StddevPop, StddevSamp, Sum, VariancePop, VarianceSamp}
import org.apache.spark.sql.types.BooleanType

case class VersionSpecificAggregateExpressionExtractor(expressionExtractor: ExpressionExtractor,
                                                       context: SQLGenContext,
                                                       filter: Option[SQLGen.Joinable]) {
  def unapply(aggFunc: AggregateFunction): Option[Statement] = {
    aggFunc match {
      // CentralMomentAgg.scala
      case StddevPop(expressionExtractor(child)) =>
        Some(aggregateWithFilter("STDDEV_POP", child, filter))
      case StddevSamp(expressionExtractor(child)) =>
        Some(aggregateWithFilter("STDDEV_SAMP", child, filter))
      case VariancePop(expressionExtractor(child)) =>
        Some(aggregateWithFilter("VAR_POP", child, filter))
      case VarianceSamp(expressionExtractor(child)) =>
        Some(aggregateWithFilter("VAR_SAMP", child, filter))

      case Kurtosis(expressionExtractor(child))  =>
        // IF(STD(child) = 0, null,
        // ( (AVG(POW(child, 4)) - AVG(child) * POW(AVG(child), 3) * 4  + 6 * AVG(POW(child), 2) * POW(AVG(child), 2)  - 3 * POW(AVG(child), 4) )
        //  / POW( AVG(POW(child, 2) - POW(AVG(child), 2) , 2) )  - 3
        // following the formula from https://stats.oarc.ucla.edu/other/mult-pkg/faq/general/faq-whats-with-the-different-formulas-for-kurtosis/ article
        Some(
          f("IF", op("=", aggregateWithFilter("STD", child, filter), "0"), StringVar(null),
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
                f("POW",
                  op("-",
                    aggregateWithFilter("AVG", f("POW", child, "2"), filter),
                    f("POW", aggregateWithFilter("AVG", child, filter), "2")),
                  "2")
              ),
              "3"
            )
          )
        )

      case Skewness(expressionExtractor(child)) =>
        // IF(STD(child) = 0, null,
        // (AVG(POW(child, 3)) - AVG(child) * POW(STD(child), 2) * 3 - POW(AVG(child), 3) ) / POW(STD(child), 3) )
        // following the definition section in https://en.wikipedia.org/wiki/Skewness
        Some(
          f("IF", op("=", aggregateWithFilter("STD", child, filter), "0"), StringVar(null),
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
        )

      // First.scala
      case First(expressionExtractor(child), Literal(false, BooleanType)) =>
        Some(aggregateWithFilter("ANY_VALUE", child, filter))

      // Last.scala
      case Last(expressionExtractor(child), Literal(false, BooleanType)) =>
        Some(aggregateWithFilter("ANY_VALUE", child, filter))

      // Sum.scala
      case Sum(expressionExtractor(child)) =>
        Some(aggregateWithFilter("SUM", child, filter))

      // Average.scala
      case Average(expressionExtractor(child)) =>
        Some(aggregateWithFilter("AVG", child, filter))

      case _ => None
    }
  }
}
