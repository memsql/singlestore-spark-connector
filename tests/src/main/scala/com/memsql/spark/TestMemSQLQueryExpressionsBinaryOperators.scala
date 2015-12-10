package com.memsql.spark

import com.memsql.spark.pushdown.MemSQLPushdownStrategy
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.memsql.test.TestUtils
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.memsql.{SparkTestUtils, MemSQLContext}
import org.apache.spark.sql.functions._

object TestMemSQLQueryExpressionsBinaryOperators {
  def main(args: Array[String]): Unit = new TestMemSQLQueryExpressionsBinaryOperators
}

class TestMemSQLQueryExpressionsBinaryOperators extends TestApp with Logging {
  def runTest(sc: SparkContext, msc: MemSQLContext): Unit = {
    val mscNoPushdown = new MemSQLContext(sc)
    MemSQLPushdownStrategy.unpatchSQLContext(mscNoPushdown)

    withStatement(stmt => {
      stmt.execute("CREATE TABLE a (a BIGINT AUTO_INCREMENT PRIMARY KEY, b BIGINT, c BIGINT)")
      stmt.execute("INSERT INTO a (b, c) VALUES (1, 1), (1, 2), (1, 3)")
    })

    val fns = Seq(Add, BitwiseAnd, BitwiseOr, BitwiseXor, MaxOf, MinOf, Multiply, Pmod, Remainder, Subtract)

    val exampleLiteral = 17
    val exprs = fns.flatMap(fn => {
      Seq(
        fn(UnresolvedAttribute.quotedString("b"), Literal(exampleLiteral)),
        fn(UnresolvedAttribute.quotedString("b"), UnresolvedAttribute.quotedString("c"))
      )
    })

    val allDFs = (
      mscNoPushdown.table("a"),
      msc.table("a")
    )

    TestUtils.runQueries[DataFrame](allDFs, {
      case a: DataFrame => {
        exprs.map(expr => {
          a.select(SparkTestUtils.exprToColumn(expr))
        })
      }
    })
  }
}
