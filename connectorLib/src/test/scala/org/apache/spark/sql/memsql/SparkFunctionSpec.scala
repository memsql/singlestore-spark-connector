// scalastyle:off magic.number regex

package org.apache.spark.sql.memsql

import com.memsql.spark.pushdown.{MemSQLPhysicalRDD, SimpleUnaryExpression}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.memsql.test.{TestData, TestUtils, SharedMemSQLContext}
import org.scalatest.{FunSpec, FlatSpec, Matchers}

import scala.util.{Failure, Success, Try}

case class TestExpression(name: String, numArgs: Int, builder: FunctionBuilder) {
  def shouldCompareResults: Boolean =
    ! Seq("rand()", "current_timestamp()", "pmod(?,?)").contains(name)
}

object LockoutReason extends Enumeration {
  type LockoutReason = Value
  val NotSupportedByMemSQL = Value
  val SparkInternalExpression = Value
  val ResultDifferentThanSpark = Value
}

import LockoutReason._

object TestExpressions {
  /**
    * functions is a list of TestExpression's
    */
  val functions = FunctionRegistry.expressions.flatMap({
    case (_, (info, builder)) => {
      Range(0, 5).flatMap(i => {
        try {
          builder(Stream.continually(Literal(1)).take(i).toSeq)
          val argsStr = ("?" * i).mkString(",")
          Some(TestExpression(s"${info.getName}($argsStr)", i, builder))
        } catch {
          // Expression doesn't support this number of arguments
          case e: AnalysisException => None
        }
      })
    }
  })

  def checkLockedOut(expr: TestExpression, inst: Expression): Option[LockoutReason] = inst match {
    case Base64(_) |
         UnBase64(_) |
         First(_) |
         Last(_) |
         Tanh(_) |
         Cosh(_) |
         Rint(_) |
         Sinh(_) |
         Expm1(_) |
         Ascii(_) |
         Crc32(_) |
         Log1p(_) |
         NaNvl(_, _) |
         Cbrt(_) |
         Pow(_, _) |
         StringReverse(_) |
         StringSpace(_) |
         InitCap(_) |
         Levenshtein(_, _) |
         SoundEx(_) |
         TruncDate(_, _) |
         Decode(_, _) |
         NaNvl(_, _) |
         DateFormatClass(_, _) |
         StringRepeat(_, _) |
         IsNaN(_) |
         NextDay(_, _) |
         FormatNumber(_, _) |
         StringSplit(_, _) |
         DateSub(_, _) |
         DateAdd(_, _) |
         DateDiff(_, _) |
         TimeAdd(_, _) |
         TimeSub(_, _) |
         AddMonths(_, _) |
         Exp(_) |
         Randn(_) |
         Hypot(_, _) |
         Encode(_, _) |
         FromUnixTime(_, _) |
         UnixTimestamp(_, _) |
         MonthsBetween(_, _) |
         ToUTCTimestamp(_, _) |
         FromUTCTimestamp(_, _) |
         RegExpExtract(_, _, _) |
         RegExpReplace(_, _, _) |
         StringTranslate(_, _, _) |
         StringLocate(_, _, _) |
         Substring(_, _, _) |
         StringRPad(_, _, _) |
         StringLPad(_, _, _) |
         Factorial(_) => Some(NotSupportedByMemSQL)

    case ConcatWs(children) if children.length <= 1 => Some(NotSupportedByMemSQL)

    case ShiftLeft(_, _) |
         ShiftRight(_, _) |
         ShiftRightUnsigned(_, _) |
         Round(_, _) => Some(ResultDifferentThanSpark)
    case _: FormatString => Some(ResultDifferentThanSpark)

    case SparkPartitionID() |
         SortArray(_, _) |
         GetArrayItem(_, _) |
         CreateArray(_) |
         CreateStruct(_) |
         CreateNamedStruct(_) |
         CreateNamedStructUnsafe(_) |
         ArrayContains(_, _) |
         GetMapValue(_, _) |
         CombineSets(_, _) |
         FindInSet(_, _) |
         InputFileName() => Some(SparkInternalExpression)

    case _ => None
  }
}

class SparkFunctionSpec extends FunSpec with SharedMemSQLContext with Matchers {
  val NUMERIC_TOLERANCE = 0.001

  var tbName: String = null
  var table: DataFrame = null
  var tableOutput: Seq[Attribute] = null

  override def beforeAll() {
    super.beforeAll()

    tbName = TestUtils.setupAllMemSQLTypes(this, TestData.memsqlTypes)
    table = msc.table(tbName).sort("val_int")
    tableOutput = table.logicalPlan.output
  }

  describe("MemSQLPushdownStrategy") {
    TestExpressions.functions.foreach(expr => {
      it(s"should support ${expr.name}") {
        val projectList = tableOutput
          .combinations(expr.numArgs) // get every combination of types for the target argument length
          .flatMap(_.permutations.toList) // permute each combination
          .flatMap(types => {
            // Build an instance of the expression with the target type combination
            // Also verify it supports this type combination
            Try(expr.builder(types)) match {
              case Success(e) => {
                if (e.checkInputDataTypes.isSuccess) {
                  Some(e)
                }
                else {
                  None
                }
              }
              case Failure(_) => None
            }
          })
          .zipWithIndex
          .map({ case (e: Expression, i: Int) => Alias(e, s"output$i")() })
          .toList

        // HAVE TO LOCK OUT get_json_object here because
        // I can't even import the f'ing case class
        if (expr.name.startsWith("get_json_object") || projectList.isEmpty) {
          cancel(s"${expr.name} is not supported by this test.")
        } else if (TestExpressions.checkLockedOut(expr, projectList(0).child).isDefined) {
          val lockoutReason = TestExpressions.checkLockedOut(expr, projectList(0).child)
          cancel(s"${expr.name} is locked out: ${lockoutReason.get}")
        } else {
          val plan = Project(projectList, table.logicalPlan)

          // Run the plan against MemSQL with/without Pushdown
          val dfPatched = DataFrame(msc, plan)
          val dfUnpatched = DataFrame(sqlContext, plan)

          val sparkPlan = dfPatched.queryExecution.sparkPlan

          if (!sparkPlan.isInstanceOf[MemSQLPhysicalRDD]) {
            fail(s"${expr.name} does not result in a single SQL query.")
          } else {
            info(s"Running ${expr.name} against Spark and MemSQL")
            info(sparkPlan.toString)

            val output = dfPatched.collect

            if (expr.shouldCompareResults) {
              // We ran the expression against MemSQL successfully,
              // now lets run it without pushdown and compare.
              info(s"Comparing results for: ${expr.name}")
              val leftOutput = output.map(_.toSeq)
              val rightOutput = dfUnpatched.collect.map(_.toSeq)

              leftOutput.zip(rightOutput).zipWithIndex.foreach({
                case ((leftRow, rightRow), i) => {
                  val niceFail = (j: Int, leftVal: Any, rightVal: Any) => {
                    info(s"Row $i is different at column $j.")
                    info(s"\t$leftRow")
                    info(s"\t$rightRow")
                    fail(s"${expr.name} Values: [$leftVal] [$rightVal]")
                  }

                  leftRow.zip(rightRow).zipWithIndex.foreach({
                    // Allow (NaN|null) == (NaN|null) for Double/Float
                    case ((null, r: Double), _) if r.isNaN =>
                    case ((l: Double, null), _) if l.isNaN =>
                    case ((null, r: Float), _) if r.isNaN =>
                    case ((l: Float, null), _) if l.isNaN =>

                    // Allow NaN == NaN for Double/Float
                    case ((l: Double, r: Double), _) if l.isNaN && r.isNaN =>
                    case ((l: Float, r: Float), _) if l.isNaN && r.isNaN =>

                    case ((l: Double, r: Double), j) => {
                      if (l !== (r +- NUMERIC_TOLERANCE)) {
                        niceFail(j, l, r)
                      }
                    }
                    case ((l: Float, r: Float), j) => {
                      if (l !== (r +- NUMERIC_TOLERANCE.toFloat)) {
                        niceFail(j, l, r)
                      }
                    }
                    case ((l, r), j) => {
                      if (l !== r) {
                        niceFail(j, l, r)
                      }
                    }
                  })
                }
              })
            }
          }
        }
      }
    })
  }
}
