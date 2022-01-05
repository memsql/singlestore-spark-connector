package com.singlestore.spark

import com.singlestore.spark.VersionSpecificExpressionGen
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

import scala.reflect.ClassTag

object ExpressionGen extends LazyLogging {
  import SQLGen._

  final val SINGLESTORE_DECIMAL_MAX_PRECISION = 65
  final val SINGLESTORE_DECIMAL_MAX_SCALE     = 30
  final val SINGLESTORE_DEFAULT_TIME_FORMAT   = UTF8String.fromString("yyyy-MM-dd HH:mm:ss")

  // DAYS_OF_WEEK_OFFSET_MAP is a map from week day prefix to it's offset (sunday -> 1, saturday -> 7)
  final val DAYS_OF_WEEK_OFFSET_MAP: Map[String, String] = {
    val daysOfWeek =
      List("sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday")
    val prefix2    = daysOfWeek.map(day => day.slice(0, 2))
    val prefix3    = daysOfWeek.map(day => day.slice(0, 3))
    val dayFormats = daysOfWeek ::: prefix3 ::: prefix2

    dayFormats.zipWithIndex.map { case (day, ind) => (day, (ind % 7 + 1).toString) }.toMap
  }

  // helpers to keep this code sane
  def f(n: String, c: Joinable*): Statement              = func(n, c: _*)
  def op(o: String, l: Joinable, r: Joinable): Statement = block(l + o + r)
  def ifNeg(value: Joinable, valTrue: Joinable, valFalse: Joinable): Statement =
    f("IF", op("<", value, IntVar(0)), valTrue, valFalse)

  def makeDecimal(child: Joinable, precision: Int, scale: Int): Joinable = {
    val p = Math.min(SINGLESTORE_DECIMAL_MAX_PRECISION, precision)
    val s = Math.min(SINGLESTORE_DECIMAL_MAX_SCALE, scale)
    cast(child, s"DECIMAL($p, $s)")
  }

  def addMicroseconds(start: Joinable, v: CalendarInterval): Joinable =
    if (v.microseconds == 0) {
      start
    } else {
      f("DATE_ADD", start, Raw("INTERVAL") + v.microseconds.toString + "MICROSECOND")
    }

  def addMonths(start: Joinable, v: CalendarInterval): Joinable =
    if (v.months == 0) {
      start
    } else {
      f("DATE_ADD", start, Raw("INTERVAL") + v.months.toString + "MONTH")
    }

  def subMicroseconds(start: Joinable, v: CalendarInterval): Joinable =
    if (v.microseconds == 0) {
      start
    } else {
      f("DATE_SUB", start, Raw("INTERVAL") + v.microseconds.toString + "MICROSECOND")
    }

  def subMonths(start: Joinable, v: CalendarInterval): Joinable =
    if (v.months == 0) {
      start
    } else {
      f("DATE_SUB", start, Raw("INTERVAL") + v.months.toString + "MONTH")
    }

  def longToDecimal(child: Joinable, p: Int, s: Int): Joinable =
    makeDecimal(op("/", child, math.pow(10.0, s).toString), p, s)

  def like(left: Joinable, right: Joinable): Joinable =
    op("LIKE", left, right)

  // regexpFromStart adds a ^ prefix for singlestore regexp to match the beginning of the string (as Java does)
  def regexpFromStart(r: Joinable): Joinable = func("CONCAT", StringVar("^"), r)

  // computeNextDay returns a statement for computing the next date after startDate with specified offset (sunday -> 1, saturday -> 7)
  // ADDDATE(startDate,(dayOfWeekOffset - DAYOFWEEK(startDate) + 6)%7 +1)
  def computeNextDay(startDate: Joinable, offset: Joinable): Statement = f(
    "ADDDATE",
    startDate,
    op(
      "+",
      op(
        "%",
        op(
          "+",
          op(
            "-",
            offset,
            f("DAYOFWEEK", startDate)
          ),
          "6"
        ),
        "7"
      ),
      "1"
    )
  )

  object GenLiteral {
    def unapply(arg: Expression): Option[Joinable] = arg match {
      case Literal(v, _) if v == null      => Some(StringVar(null))
      case Literal(v: Int, DateType)       => Some(DateVar(DateTimeUtils.toJavaDate(v)))
      case Literal(v: Long, TimestampType) => Some(TimestampVar(DateTimeUtils.toJavaTimestamp(v)))

      case Literal(v, _) => convertLiteralValue.lift(v)

      case _ => None
    }

    def unapply(vals: Iterable[Any]): Option[Joinable] =
      vals
        .map(convertLiteralValue.lift)
        .reduce[Option[Joinable]] {
          case (Some(left), Some(right)) => Some(left + "," + right)
          case _                         => None
        }

    def convertLiteralValue: PartialFunction[Any, Joinable] = {
      case v if v == null => StringVar(null)

      case v: String     => StringVar(v)
      case v: UTF8String => StringVar(v.toString)
      case v: Byte       => ByteVar(v)

      case v: Boolean => Raw(if (v) "TRUE" else "FALSE")

      case v: Short                                  => Raw(v.toString)
      case v: Int                                    => Raw(v.toString)
      case v: Integer                                => Raw(v.toString)
      case v: Long                                   => Raw(v.toString)
      case v: Decimal                                => makeDecimal(Raw(v.toString), v.precision, v.scale)
      case v: BigDecimal                             => makeDecimal(Raw(v.toString), v.precision, v.scale)
      case v: Float if java.lang.Float.isFinite(v)   => Raw(v.toString)
      case v: Double if java.lang.Double.isFinite(v) => Raw(v.toString)
    }
  }

  case class FoldableExtractor[T]() {
    def unapply(e: Expression)(implicit tag: ClassTag[T]): Option[T] =
      if (e.foldable) {
        e.eval() match {
          case expr: T =>
            tag.unapply(expr)
          case _ => None
        }
      } else None
  }

  case class DecimalExpressionExtractor(expressionExtractor: ExpressionExtractor) {
    def unapply(e: Expression): Option[(Joinable, Int, Int)] = (e, e.dataType) match {
      case (expressionExtractor(child), t: DecimalType) => Some((child, t.precision, t.scale))
      case _                                            => None
    }
  }

  case class WindowBoundaryExpressionExtractor(expressionExtractor: ExpressionExtractor) {
    def unapply(arg: Expression): Option[Joinable] = arg match {
      case e: SpecialFrameBoundary                => Some(e.sql)
      case UnaryMinus(expressionExtractor(child)) => Some(child + "PRECEDING")
      case Literal(n: Integer, IntegerType) =>
        Some(Raw(Math.abs(n).toString) + (if (n < 0) "PRECEDING" else "FOLLOWING"))
      case expressionExtractor(child) => Some(child + "FOLLOWING")
      case _                          => None
    }
  }

  case class AggregateExpressionExtractor(expressionExtractor: ExpressionExtractor,
                                          context: SQLGenContext) {
    def unapply(arg: AggregateExpression): Option[Joinable] = {
      val filterOption = AggregateExpressionFilter.get(expressionExtractor, arg)
      filterOption.flatMap(filter => {
        val bitAggregateExpressionExtractor =
          BitAggregateExpressionExtractor(expressionExtractor, context, filter)
        arg.aggregateFunction match {

          // Average.scala
          case Average(expressionExtractor(child)) =>
            Some(aggregateWithFilter("AVG", child, filter))

          // CentralMomentAgg.scala
          case StddevPop(expressionExtractor(child)) =>
            Some(aggregateWithFilter("STDDEV_POP", child, filter))
          case StddevSamp(expressionExtractor(child)) =>
            Some(aggregateWithFilter("STDDEV_SAMP", child, filter))
          case VariancePop(expressionExtractor(child)) =>
            Some(aggregateWithFilter("VAR_POP", child, filter))
          case VarianceSamp(expressionExtractor(child)) =>
            Some(aggregateWithFilter("VAR_SAMP", child, filter))

          // TODO: case Skewness(expressionExtractor(child))     => ???
          // TODO: case Kurtosis(expressionExtractor(child))     => ???

          case Count(expression) =>
            (expression, arg.isDistinct, filter) match {
              case (expressionExtractor(None), false, filter) =>
                Some(aggregateWithFilter("COUNT", "1", filter))
              case (expressionExtractor(Some(children)), false, filter) =>
                Some(aggregateWithFilter("COUNT", children, filter))
              // DISTINCT and FILTER can't be used together
              case (expressionExtractor(Some(children)), true, None) =>
                Some(Raw("COUNT") + block(Raw("DISTINCT") + children))
              case _ => None
            }

          // Covariance.scala
          // TODO: case CovPopulation(expressionExtractor(left), expressionExtractor(right)) => ???
          // TODO: case CovSample(expressionExtractor(left), expressionExtractor(right))     => ???

          // First.scala
          case First(expressionExtractor(child), false) =>
            Some(aggregateWithFilter("ANY_VALUE", child, filter))

          // Last.scala
          case Last(expressionExtractor(child), false) =>
            Some(aggregateWithFilter("ANY_VALUE", child, filter))

          // Max.scala
          case Max(expressionExtractor(child)) =>
            Some(aggregateWithFilter("MAX", child, filter))

          // Min.scala
          case Min(expressionExtractor(child)) =>
            Some(aggregateWithFilter("MIN", child, filter))

          // Sum.scala
          case Sum(expressionExtractor(child)) =>
            Some(aggregateWithFilter("SUM", child, filter))

          case bitAggregateExpressionExtractor(statement) => Some(statement)
          //    case AggregateExpression(MaxBy(expressionExtractor(valueExpr), expressionExtractor(orderingExpr)), _, _, None, _) =>
          //    case AggregateExpression(MinBy(expressionExtractor(valueExpr), expressionExtractor(orderingExpr)), _, _, None, _) =>
          case _ => None
        }
      })
    }
  }

  // we need to manually unwrap MonthsBetween since the roundOff argument
  // does not exist in Spark 2.3
  // The roundOff argument truncates the result to 8 digits of precision
  // which we can safely ignore, the user can apply an explicit round if needed
  case class MonthsBetweenExpressionExtractor(expressionExtractor: ExpressionExtractor) {
    def unapply(arg: MonthsBetween): Option[(Joinable, Joinable)] =
      for {
        date1 <- expressionExtractor.unapply(arg.date1)
        date2 <- expressionExtractor.unapply(arg.date2)
      } yield (date1, date2)
  }

  def aggregateWithFilter(funcName: String, child: Joinable, filter: Option[Joinable]) = {
    filter match {
      case Some(filterExpression) =>
        f(funcName, f("IF", filterExpression, child, StringVar(null)))
      case None => f(funcName, child)
    }
  }

  def apply(expressionExtractor: ExpressionExtractor): PartialFunction[Expression, Joinable] = {
    val windowBoundaryExpressionExtractor = WindowBoundaryExpressionExtractor(expressionExtractor)
    val monthsBetweenExpressionExtractor  = MonthsBetweenExpressionExtractor(expressionExtractor)
    val context                           = expressionExtractor.context
    val aggregateExpressionExtractor      = AggregateExpressionExtractor(expressionExtractor, context)
    val decimalExpressionExtractor        = DecimalExpressionExtractor(expressionExtractor)
    val intFoldableExtractor              = FoldableExtractor[Int]()
    val utf8StringFoldableExtractor       = FoldableExtractor[UTF8String]()
    val versionSpecificExpressionGen      = VersionSpecificExpressionGen(expressionExtractor)

    return {
      // ----------------------------------
      // Attributes
      // ----------------------------------
      case a: Attribute => Attr(a, context)
      case a @ Alias(expressionExtractor(child), name) =>
        alias(child, name, a.exprId, context)

      // ----------------------------------
      // Literals
      // ----------------------------------
      case GenLiteral(v) => v

      // ----------------------------------
      // Variable Expressions
      // ----------------------------------

      case Coalesce(expressionExtractor(Some(child))) => f("COALESCE", child)
      case Least(expressionExtractor(Some(child)))    => f("LEAST", child)
      case Greatest(expressionExtractor(Some(child))) => f("GREATEST", child)
      case Concat(expressionExtractor(Some(child)))   => f("CONCAT", child)
      case Elt(expressionExtractor(Some(child)))      => f("ELT", child)

      // ----------------------------------
      // Aggregate Expressions
      // ----------------------------------
      case aggregateExpressionExtractor(expression) => expression

      // windowExpressions.scala
      case WindowExpression(expressionExtractor(child),
                            WindowSpecDefinition(expressionExtractor(partitionSpec),
                                                 expressionExtractor(orderSpec),
                                                 expressionExtractor(frameSpec))) =>
        child + "OVER" + block(
          partitionSpec.map(Raw("PARTITION BY") + _).getOrElse(empty) +
            orderSpec.map(Raw("ORDER BY") + _).getOrElse(empty) +
            frameSpec
        )

      case UnspecifiedFrame => ""

      case SpecifiedWindowFrame(frameType,
                                windowBoundaryExpressionExtractor(lower),
                                windowBoundaryExpressionExtractor(upper)) =>
        Raw(frameType.sql) + "BETWEEN" + lower + "AND" + upper

      case Lead(expressionExtractor(input), expressionExtractor(offset), Literal(null, NullType)) =>
        f("LEAD", input, offset)
      case Lag(expressionExtractor(input), expressionExtractor(offset), Literal(null, NullType)) =>
        f("LAG", input, offset)
      case RowNumber()                       => "ROW_NUMBER()"
      case NTile(expressionExtractor(child)) => f("NTILE", child)
      case Rank(_)                           => "RANK()"
      case DenseRank(_)                      => "DENSE_RANK()"
      case PercentRank(_)                    => "PERCENT_RANK()"

      // TODO: case CumeDist()               => ???

      // ----------------------------------
      // Binary Expressions
      // ----------------------------------

      // arithmetic.scala

      case Add(expressionExtractor(left), expressionExtractor(right))       => op("+", left, right)
      case Subtract(expressionExtractor(left), expressionExtractor(right))  => op("-", left, right)
      case Multiply(expressionExtractor(left), expressionExtractor(right))  => op("*", left, right)
      case Divide(expressionExtractor(left), expressionExtractor(right))    => op("/", left, right)
      case Remainder(expressionExtractor(left), expressionExtractor(right)) => op("%", left, right)

      case Pmod(expressionExtractor(left), expressionExtractor(right)) =>
        block(block(block(left + "%" + right) + "+" + right) + "%" + right)

      // bitwiseExpressions.scala
      case BitwiseAnd(expressionExtractor(left), expressionExtractor(right)) => op("&", left, right)
      case BitwiseOr(expressionExtractor(left), expressionExtractor(right))  => op("|", left, right)
      case BitwiseXor(expressionExtractor(left), expressionExtractor(right)) => op("^", left, right)

      // datetimeExpressions.scala

      // NOTE: we explicitly ignore the timeZoneId field in all of the following expressions
      // The user is required to setup Spark and/or SingleStore with the timezone they want or they
      // will get inconsistent results with/without pushdown.

      case DateAdd(expressionExtractor(startDate), expressionExtractor(days)) =>
        f("ADDDATE", startDate, days)
      case DateSub(expressionExtractor(startDate), expressionExtractor(days)) =>
        f("SUBDATE", startDate, days)

      case FromUTCTimestamp(expressionExtractor(timestamp), expressionExtractor(timezone)) =>
        f("CONVERT_TZ", timestamp, StringVar("UTC"), timezone)

      case ToUTCTimestamp(expressionExtractor(timestamp), expressionExtractor(timezone)) =>
        f("CONVERT_TZ", timestamp, timezone, StringVar("UTC"))

      case TruncTimestamp(expressionExtractor(format),
                          expressionExtractor(timestamp),
                          timeZoneId) => {
        f(
          "DATE_TRUNC",
          sqlMapValueCaseInsensitive(
            format,
            Map(
              // SingleStore doesn't support formats ("yyyy", "yy", "mon", "mm", "dd") so we map them here
              "yyyy" -> "year",
              "yy"   -> "year",
              "mon"  -> "month",
              "mm"   -> "month",
              "dd"   -> "day"
            ),
            format
          ),
          timestamp
        )
      }

      case TruncDate(expressionExtractor(date), expressionExtractor(format)) => {
        f(
          "DATE_TRUNC",
          sqlMapValueCaseInsensitive(
            format,
            Map(
              // SingleStore doesn't support formats ("yyyy", "yy", "mon", "mm") so we map them here
              "yyyy" -> "year",
              "yy"   -> "year",
              "mon"  -> "month",
              "mm"   -> "month"
            ),
            format
          ),
          date
        )
      }

      case monthsBetweenExpressionExtractor((date1, date2)) =>
        f("MONTHS_BETWEEN", date1, date2)

      case AddMonths(expressionExtractor(startDate), expressionExtractor(numMonths)) =>
        f("DATE_ADD", startDate, Raw("INTERVAL") + numMonths + "MONTH")

      // SingleStore and spark support other date formats
      // UnixTime doesn't use format if time is already a dataType or TimestampType
      case ToUnixTimestamp(e @ expressionExtractor(timeExp), _, _) if e.dataType == DateType =>
        f("UNIX_TIMESTAMP", timeExp)

      case ToUnixTimestamp(e @ expressionExtractor(timeExp), _, _) if e.dataType == TimestampType =>
        f("ROUND", f("UNIX_TIMESTAMP", timeExp), "0")

      case UnixTimestamp(e @ expressionExtractor(timeExp), _, _) if e.dataType == DateType =>
        f("UNIX_TIMESTAMP", timeExp)

      case UnixTimestamp(e @ expressionExtractor(timeExp), _, _) if e.dataType == TimestampType =>
        f("ROUND", f("UNIX_TIMESTAMP", timeExp), "0")

      case FromUnixTime(expressionExtractor(sec), utf8StringFoldableExtractor(format), timeZoneId)
          if format == SINGLESTORE_DEFAULT_TIME_FORMAT =>
        f("FROM_UNIXTIME", sec)

      case NextDay(expressionExtractor(startDate), utf8StringFoldableExtractor(dayOfWeek)) =>
        computeNextDay(
          startDate,
          sqlMapValueCaseInsensitive(
            StringVar(dayOfWeek.toString),
            DAYS_OF_WEEK_OFFSET_MAP,
            StringVar(null)
          )
        )

      case NextDay(expressionExtractor(startDate), expressionExtractor(dayOfWeek)) =>
        computeNextDay(startDate,
                       sqlMapValueCaseInsensitive(
                         dayOfWeek,
                         DAYS_OF_WEEK_OFFSET_MAP,
                         StringVar(null)
                       ))

      case DateDiff(expressionExtractor(endDate), expressionExtractor(startDate)) =>
        f("DATEDIFF", endDate, startDate)

      // mathExpressions.scala
      case Atan2(expressionExtractor(left), expressionExtractor(right))     => f("ATAN2", left, right)
      case Pow(expressionExtractor(left), expressionExtractor(right))       => f("POWER", left, right)
      case ShiftLeft(expressionExtractor(left), expressionExtractor(right)) => op("<<", left, right)
      case ShiftRight(expressionExtractor(left), expressionExtractor(right)) =>
        op(">>", left, right)
      case ShiftRightUnsigned(expressionExtractor(left), expressionExtractor(right)) =>
        op(">>", left, right)
      case Logarithm(expressionExtractor(left), expressionExtractor(right)) => f("LOG", left, right)
      case Round(expressionExtractor(child), expressionExtractor(scale))    => f("ROUND", child, scale)
      case Hypot(expressionExtractor(left), expressionExtractor(right)) =>
        f("SQRT", op("+", f("POW", left, "2"), f("POW", right, "2")))

      // TODO: case _: BRound => None

      // predicates.scala
      case And(expressionExtractor(left), expressionExtractor(right)) => op("AND", left, right)
      case Or(expressionExtractor(left), expressionExtractor(right))  => op("OR", left, right)

      case EqualTo(expressionExtractor(left), expressionExtractor(right)) => op("=", left, right)
      case EqualNullSafe(expressionExtractor(left), expressionExtractor(right)) =>
        op("<=>", left, right)
      case LessThan(expressionExtractor(left), expressionExtractor(right)) => op("<", left, right)
      case LessThanOrEqual(expressionExtractor(left), expressionExtractor(right)) =>
        op("<=", left, right)
      case GreaterThan(expressionExtractor(left), expressionExtractor(right)) =>
        op(">", left, right)
      case GreaterThanOrEqual(expressionExtractor(left), expressionExtractor(right)) =>
        op(">=", left, right)

      case If(expressionExtractor(predicate),
              expressionExtractor(trueValue),
              expressionExtractor(falseValue)) =>
        f("IF", predicate, trueValue, falseValue)

      case In(expressionExtractor(child), expressionExtractor(Some(elements))) =>
        op("IN", child, block(elements))

      case InSet(expressionExtractor(child), GenLiteral(elements)) =>
        op(
          "IN",
          child,
          block(elements)
        )

      // regexpExpressions.scala
      case RLike(expressionExtractor(left), expressionExtractor(right)) =>
        op("RLIKE", left, regexpFromStart(right))

      // stringExpressions.scala
      case Contains(expressionExtractor(left), expressionExtractor(right)) =>
        op(">", f("INSTR", left, right), "0")
      case StartsWith(expressionExtractor(left), expressionExtractor(right)) =>
        op("LIKE", left, f("CONCAT", right, StringVar("%")))
      case EndsWith(expressionExtractor(left), expressionExtractor(right)) =>
        op("LIKE", left, f("CONCAT", StringVar("%"), right))
      case StringInstr(expressionExtractor(str), expressionExtractor(substr)) =>
        f("INSTR", str, substr)
      case FormatNumber(expressionExtractor(x), e @ expressionExtractor(d))
          if e.dataType == IntegerType =>
        ifNeg(d, StringVar(null), f("FORMAT", x, d))
      case StringRepeat(expressionExtractor(child), expressionExtractor(times)) =>
        f("LPAD",
          StringVar(""),
          ifNeg(times, IntVar(0), times) + "*" + f("CHAR_LENGTH", child),
          child)

      case StringTrim(expressionExtractor(srcStr), None) =>
        f("TRIM", Raw("BOTH") + "FROM" + srcStr)
      case StringTrim(expressionExtractor(srcStr), Some(utf8StringFoldableExtractor(trimStr)))
          if trimStr == UTF8String.fromString(" ") =>
        f("TRIM", Raw("BOTH") + "FROM" + srcStr)

      case StringTrimLeft(expressionExtractor(srcStr), None) =>
        f("LTRIM", srcStr)
      case StringTrimLeft(expressionExtractor(srcStr), Some(utf8StringFoldableExtractor(trimStr)))
          if trimStr == UTF8String.fromString(" ") =>
        f("LTRIM", srcStr)

      case StringTrimRight(expressionExtractor(srcStr), None) =>
        f("RTRIM", srcStr)
      case StringTrimRight(expressionExtractor(srcStr), Some(utf8StringFoldableExtractor(trimStr)))
          if trimStr == UTF8String.fromString(" ") =>
        f("RTRIM", srcStr)

      // TODO: case _: Levenshtein => None

      // ----------------------------------
      // Leaf Expressions
      // ----------------------------------

      // datetimeExpressions.scala
      case CurrentDate(_)     => "CURRENT_DATE()"
      case CurrentTimestamp() => "NOW(6)"

      // mathExpressions.scala
      case EulerNumber() => math.E.toString
      case Pi()          => "PI()"

      // ----------------------------------
      // Ternary Expressions
      // ----------------------------------

      // mathExpressions.scala
      case Conv(expressionExtractor(numExpr),
                intFoldableExtractor(fromBase),
                intFoldableExtractor(toBase))
          // SingleStore supports bases only from [2, 36]
          if fromBase >= 2 && fromBase <= 36 &&
            toBase >= 2 && toBase <= 36 =>
        f("CONV", numExpr, IntVar(fromBase), IntVar(toBase))

      // regexpExpression.scala
      case RegExpReplace(expressionExtractor(subject),
                         expressionExtractor(regexp),
                         expressionExtractor(rep)) =>
        f("REGEXP_REPLACE", subject, regexp, rep)

      // TODO: case RegExpExtract(expressionExtractor(subject), expressionExtractor(regexp), expressionExtractor(idx)) => ???

      // stringExpressions.scala
      case StringReplace(expressionExtractor(srcExpr),
                         expressionExtractor(searchExpr),
                         expressionExtractor(replaceExpr)) =>
        f("REPLACE", srcExpr, searchExpr, replaceExpr)
      case SubstringIndex(expressionExtractor(strExpr),
                          expressionExtractor(delimExpr),
                          expressionExtractor(countExpr)) =>
        f("SUBSTRING_INDEX", strExpr, delimExpr, countExpr)
      case StringLocate(expressionExtractor(substr),
                        expressionExtractor(str),
                        expressionExtractor(start)) =>
        f("LOCATE", substr, str, start)
      case StringLPad(expressionExtractor(str),
                      expressionExtractor(len),
                      expressionExtractor(pad)) =>
        f("LPAD", str, ifNeg(len, IntVar(0), len), pad)
      case StringRPad(expressionExtractor(str),
                      expressionExtractor(len),
                      expressionExtractor(pad)) =>
        f("RPAD", str, ifNeg(len, IntVar(0), len), pad)
      case Substring(expressionExtractor(str),
                     expressionExtractor(pos),
                     expressionExtractor(len)) =>
        f("SUBSTR", str, pos, len)

      // TODO: case StringTranslate(expressionExtractor(srcExpr), expressionExtractor(matchingExpr), expressionExtractor(replaceExpr)) => ???

      // ----------------------------------
      // Unary Expressions
      // ----------------------------------

      // arithmetic.scala
      case UnaryMinus(expressionExtractor(child))    => f("-", child)
      case UnaryPositive(expressionExtractor(child)) => f("+", child)
      case Abs(expressionExtractor(child))           => f("ABS", child)

      // bitwiseExpression.scala
      case BitwiseNot(expressionExtractor(expr)) => f("~", expr)

      // Cast.scala
      case Cast(e @ expressionExtractor(child), dataType, _) => {
        dataType match {
          case TimestampType => {
            e.dataType match {
              case _: BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType |
                  DoubleType | _: DecimalType =>
                cast(f("FROM_UNIXTIME", child), "DATETIME(6)")
              case _ => cast(child, "DATETIME(6)")
            }
          }
          case DateType => cast(child, "DATE")

          case StringType => cast(child, "CHAR")
          case BinaryType => cast(child, "BINARY")

          case _: BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType |
              DoubleType | _: DecimalType =>
            if (e.dataType == DateType) {
              StringVar(null)
            } else {
              val numeric_ch = if (e.dataType == TimestampType) {
                f("UNIX_TIMESTAMP", child)
              } else {
                child
              }
              dataType match {
                case BooleanType     => op("!=", numeric_ch, IntVar(0))
                case ByteType        => op("!:>", numeric_ch, "TINYINT")
                case ShortType       => op("!:>", numeric_ch, "SMALLINT")
                case IntegerType     => op("!:>", numeric_ch, "INT")
                case LongType        => op("!:>", numeric_ch, "BIGINT")
                case FloatType       => op("!:>", numeric_ch, "FLOAT")
                case DoubleType      => op("!:>", numeric_ch, "DOUBLE")
                case dt: DecimalType => makeDecimal(numeric_ch, dt.precision, dt.scale)
              }
            }
          // SingleStore doesn't know how to handle this cast, pass it through AS is
          case _ => child
        }
      }
      // TODO: case UpCast(expressionExtractor(child), dataType, walkedTypePath) => ???

      // datetimeExpressions.scala
      case Hour(expressionExtractor(child), _)     => f("HOUR", child)
      case Minute(expressionExtractor(child), _)   => f("MINUTE", child)
      case Second(expressionExtractor(child), _)   => f("SECOND", child)
      case DayOfYear(expressionExtractor(child))   => f("DAYOFYEAR", child)
      case Year(expressionExtractor(child))        => f("YEAR", child)
      case Quarter(expressionExtractor(child))     => f("QUARTER", child)
      case Month(expressionExtractor(child))       => f("MONTH", child)
      case DayOfMonth(expressionExtractor(child))  => f("DAY", child)
      case DayOfWeek(expressionExtractor(child))   => f("DAYOFWEEK", child)
      case WeekOfYear(expressionExtractor(child))  => f("WEEK", child, "3")
      case LastDay(expressionExtractor(startDate)) => f("LAST_DAY", startDate)

      case UnscaledValue(decimalExpressionExtractor(child, precision, scale)) =>
        op("!:>", op("*", child, math.pow(10.0, scale).toString), "BIGINT")

      // hash.scala
      case Md5(expressionExtractor(child))  => f("MD5", child)
      case Sha1(expressionExtractor(child)) => f("SHA1", child)
      case Sha2(expressionExtractor(left), right)
          if right.foldable &&
            right.eval().isInstanceOf[Int] &&
            right.eval().asInstanceOf[Int] != 224 =>
        f("SHA2", left, right.toString)
      case Crc32(expressionExtractor(child)) => f("CRC32", child)

      // mathExpressions.scala
      case Acos(expressionExtractor(child))      => f("ACOS", child)
      case Asin(expressionExtractor(child))      => f("ASIN", child)
      case Atan(expressionExtractor(child))      => f("ATAN", child)
      case Ceil(expressionExtractor(child))      => f("CEIL", child)
      case Cos(expressionExtractor(child))       => f("COS", child)
      case Exp(expressionExtractor(child))       => f("EXP", child)
      case Expm1(expressionExtractor(child))     => block(func("EXP", child) + "- 1")
      case Floor(expressionExtractor(child))     => f("FLOOR", child)
      case Log(expressionExtractor(child))       => f("LOG", child)
      case Log2(expressionExtractor(child))      => f("LOG2", child)
      case Log10(expressionExtractor(child))     => f("LOG10", child)
      case Log1p(expressionExtractor(child))     => f("LOG", child + "+ 1")
      case Signum(expressionExtractor(child))    => f("SIGN", child)
      case Sin(expressionExtractor(child))       => f("SIN", child)
      case Sqrt(expressionExtractor(child))      => f("SQRT", child)
      case Tan(expressionExtractor(child))       => f("TAN", child)
      case Cot(expressionExtractor(child))       => f("COT", child)
      case ToDegrees(expressionExtractor(child)) => f("DEGREES", child)
      case ToRadians(expressionExtractor(child)) => f("RADIANS", child)
      case Bin(expressionExtractor(child))       => f("BIN", child)
      case Hex(expressionExtractor(child))       => f("HEX", child)
      case Unhex(expressionExtractor(child))     => f("UNHEX", child)

      //    case BoolAnd(expressionExtractor(arg)) => // Spark can't apply bool_and to smallint (Input to function 'bool_and' should have been boolean, but it's [smallint])
      //    case BoolOr(expressionExtractor(arg))  => // Spark can't apply bool_or to smallint (Input to function 'bool_or' should have been boolean, but it's [smallint])
      //    case ArrayForAll(expressionExtractor(arg), expressionExtractor(function))                    => ???
      //    case SchemaOfCsv(expressionExtractor(child), options)                               => ???
      //    case MapEntries(expressionExtractor(child))                                         => ???
      //    case MapFilter(expressionExtractor(arg), expressionExtractor(function))                      => ???
      //    case MapZipWith(expressionExtractor(left), expressionExtractor(right), expressionExtractor(function)) => ???
      //    case CsvToStructs(schema, options, expressionExtractor(child), timeZoneId)          => ???
      //    case StructsToCsv(options, expressionExtractor(child), timeZoneId)                  => ???
      //    case SparkVersion()                                                        => ???
      //    case TransformKeys(expressionExtractor(argument), expressionExtractor(function))             => ???
      //    case TransformValues(expressionExtractor(argument), expressionExtractor(function))           => ???
      //    case XxHash64(children, seed) => // we have 32-bit hash, but don't have 64-bit

      // tanh(x) = (exp(x) - exp(-x)) / (exp(x) + exp(-x))
      case Tanh(expressionExtractor(child)) =>
        op("/",
           op("-", f("EXP", child), f("EXP", f("-", child))),
           op("+", f("EXP", child), f("EXP", f("-", child))))

      // sinh(x) = (exp(x) - exp(-x)) / 2
      case Sinh(expressionExtractor(child)) =>
        op("/", op("-", f("EXP", child), f("EXP", f("-", child))), "2")

      // cosh(x) = (exp(x) + exp(-x)) / 2
      case Cosh(expressionExtractor(child)) =>
        op("/", op("+", f("EXP", child), f("EXP", f("-", child))), "2")
      case Rint(expressionExtractor(child)) => f("ROUND", child, "0")

      // TODO: case Factorial(expressionExtractor(child)) => ???
      // TODO: case Cbrt(expressionExtractor(child))      => f("POW", child, op("/", "1", "3"))
      //  We need to wait for the engine to implement precise cbrt

      // nullExpressions.scala
      case IfNull(expressionExtractor(left), expressionExtractor(right), _) =>
        f("COALESCE", left, right)
      case NullIf(expressionExtractor(left), expressionExtractor(right), _) =>
        f("NULLIF", left, right)
      case Nvl(expressionExtractor(left), expressionExtractor(right), _) =>
        f("COALESCE", left, right)
      case IsNull(expressionExtractor(child))    => block(child) + "IS NULL"
      case IsNotNull(expressionExtractor(child)) => block(child) + "IS NOT NULL"

      case Nvl2(expressionExtractor(expr1),
                expressionExtractor(expr2),
                expressionExtractor(expr3),
                _) =>
        f("IF", expr1 + "IS NOT NULL", expr2, expr3)

      // predicates.scala
      case Not(expressionExtractor(child)) => block(Raw("NOT") + child)
      case If(expressionExtractor(predicate),
              expressionExtractor(trueValue),
              expressionExtractor(falseValue)) =>
        f("IF", predicate, trueValue, falseValue)

      // randomExpression.scala
      case Rand(expressionExtractor(child)) => f("RAND", child)
      // TODO: case Randn(expressionExtractor(child)) => ???

      // SortOrder.scala
      // in SingleStore, nulls always come first when direction = ascending
      case SortOrder(expressionExtractor(child), Ascending, NullsFirst, _) => block(child) + "ASC"
      // in SingleStore, nulls always come last when direction = descending
      case SortOrder(expressionExtractor(child), Descending, NullsLast, _) => block(child) + "DESC"

      // stringExpressions.scala
      case Upper(expressionExtractor(child)) => f("UPPER", child)
      case Lower(expressionExtractor(child)) => f("LOWER", child)

      case StringSpace(expressionExtractor(child)) =>
        f("LPAD", StringVar(""), child, StringVar(" "))

      case Length(expressionExtractor(child))      => f("CHAR_LENGTH", child)
      case BitLength(expressionExtractor(child))   => block(func("LENGTH", child) + "* 8")
      case OctetLength(expressionExtractor(child)) => f("LENGTH", child)
      case Ascii(expressionExtractor(child))       => f("ASCII", child)
      case Chr(expressionExtractor(child)) =>
        f("IF", f("ISNULL", child), StringVar(null), f("CHAR", child))
      case Base64(expressionExtractor(child))   => f("TO_BASE64", child)
      case UnBase64(expressionExtractor(child)) => f("FROM_BASE64", child)

      case versionSpecificExpressionGen(child) => child

      // TODO: case InitCap(expressionExtractor(child)) => ???
      // TODO: case StringReverse(expressionExtractor(child)) => ???
      // TODO: case SoundEx(expressionExtractor(child)) => ???
    }
  }
}
