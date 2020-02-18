package com.memsql.spark

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object ExpressionGen extends LazyLogging {
  import SQLGen._

  final val MEMSQL_DECIMAL_MAX_PRECISION = 65
  final val MEMSQL_DECIMAL_MAX_SCALE     = 30

  // helpers to keep this code sane
  def f(n: String, c: Joinable*)              = func(n, c: _*)
  def op(o: String, l: Joinable, r: Joinable) = block(l + o + r)

  def makeDecimal(child: Joinable, precision: Int, scale: Int): Joinable = {
    val p = Math.min(MEMSQL_DECIMAL_MAX_PRECISION, precision)
    val s = Math.min(MEMSQL_DECIMAL_MAX_SCALE, scale)
    cast(child, s"DECIMAL($p, $s)")
  }

  def convertLiteral(value: Any): Joinable = value match {
    case v if v == null => StringVar(null)

    case v: String     => StringVar(v)
    case v: UTF8String => StringVar(v.toString)
    case v: Byte       => ByteVar(v)

    case v: Boolean => Raw(if (v) "TRUE" else "FALSE")

    case v: Short                                  => Raw(v.toString)
    case v: Int                                    => Raw(v.toString)
    case v: Integer                                => Raw(v.toString)
    case v: Long                                   => Raw(v.toString)
    case v: Decimal                                => Raw(v.toString)
    case v: BigDecimal                             => Raw(v.toString)
    case v: Float if java.lang.Float.isFinite(v)   => Raw(v.toString)
    case v: Double if java.lang.Double.isFinite(v) => Raw(v.toString)

    case v => {
      log.trace(s"failed to convert literal ${v} with type ${v.getClass}")
      StringVar(v.toString)
    }
  }

  object WindowBoundaryExpression {
    def unapply(arg: Expression): Option[Joinable] = arg match {
      case e: SpecialFrameBoundary       => Some(e.sql)
      case UnaryMinus(Expression(child)) => Some(child + "PRECEDING")
      case Literal(n: Integer, IntegerType) =>
        Some(Raw(Math.abs(n).toString) + (if (n < 0) "PRECEDING" else "FOLLOWING"))
      case Expression(child) => Some(child + "FOLLOWING")
      case _                 => None
    }
  }

  def apply: PartialFunction[Expression, Joinable] = {
    // ----------------------------------
    // Attributes
    // ----------------------------------
    case a: Attribute                       => Attr(a)
    case a @ Alias(Expression(child), name) => alias(child, name, a.exprId)

    // ----------------------------------
    // Literals
    // ----------------------------------
    case Literal(v, _) if v == null => StringVar(null)
    case Literal(v: Int, DateType)  => DateVar(DateTimeUtils.toJavaDate(v))
    case Literal(v: Long, TimestampType) =>
      TimestampVar(DateTimeUtils.toJavaTimestamp(v))
    case Literal(v, _) => convertLiteral(v)

    // ----------------------------------
    // Variable Expressions
    // ----------------------------------

    case Coalesce(Expression(Some(child))) => f("COALESCE", child)
    case Least(Expression(Some(child)))    => f("LEAST", child)
    case Greatest(Expression(Some(child))) => f("GREATEST", child)
    case Concat(Expression(Some(child)))   => f("CONCAT", child)
    case Elt(Expression(Some(child)))      => f("ELT", child)

    // ----------------------------------
    // Aggregate Expressions
    // ----------------------------------

    // Average.scala
    case AggregateExpression(Average(Expression(child)), _, _, _) => f("AVG", child)

    // CentralMomentAgg.scala
    case AggregateExpression(StddevPop(Expression(child)), _, _, _)    => f("STDDEV_POP", child)
    case AggregateExpression(StddevSamp(Expression(child)), _, _, _)   => f("STDDEV_SAMP", child)
    case AggregateExpression(VariancePop(Expression(child)), _, _, _)  => f("VAR_POP", child)
    case AggregateExpression(VarianceSamp(Expression(child)), _, _, _) => f("VAR_SAMP", child)

    // TODO: case Skewness(Expression(child))     => ???
    // TODO: case Kurtosis(Expression(child))     => ???

    // Count.scala
    case AggregateExpression(Count(Expression(None)), _, false, _) => Raw("COUNT(*)")
    case AggregateExpression(Count(Expression(Some(children))), _, isDistinct, _) =>
      if (isDistinct) {
        Raw("COUNT") + block(Raw("DISTINCT") + children)
      } else {
        f("COUNT", children)
      }

    // Covariance.scala
    // TODO: case CovPopulation(Expression(left), Expression(right)) => ???
    // TODO: case CovSample(Expression(left), Expression(right))     => ???

    // First.scala
    case AggregateExpression(First(Expression(child), Literal(false, BooleanType)), _, _, _) =>
      f("ANY_VALUE", child)

    // Last.scala
    case AggregateExpression(Last(Expression(child), Literal(false, BooleanType)), _, _, _) =>
      f("ANY_VALUE", child)

    // Max.scala
    case AggregateExpression(Max(Expression(child)), _, _, _) => f("MAX", child)

    // Min.scala
    case AggregateExpression(Min(Expression(child)), _, _, _) => f("MIN", child)

    // Sum.scala
    case AggregateExpression(Sum(Expression(child)), _, _, _) => f("SUM", child)

    // windowExpressions.scala
    case WindowExpression(Expression(child),
                          WindowSpecDefinition(Expression(partitionSpec),
                                               Expression(orderSpec),
                                               Expression(frameSpec))) =>
      child + "OVER" + block(
        partitionSpec.map(Raw("PARTITION BY") + _).getOrElse(empty) +
          orderSpec.map(Raw("ORDER BY") + _).getOrElse(empty) +
          frameSpec
      )

    case UnspecifiedFrame => ""

    case SpecifiedWindowFrame(frameType,
                              WindowBoundaryExpression(lower),
                              WindowBoundaryExpression(upper)) =>
      Raw(frameType.sql) + "BETWEEN" + lower + "AND" + upper

    case Lead(Expression(input), Expression(offset), Literal(null, NullType)) =>
      f("LEAD", input, offset)
    case Lag(Expression(input), Expression(offset), Literal(null, NullType)) =>
      f("LAG", input, offset)
    case RowNumber()              => "ROW_NUMBER()"
    case NTile(Expression(child)) => f("NTILE", child)
    case Rank(_)                  => "RANK()"
    case DenseRank(_)             => "DENSE_RANK()"
    case PercentRank(_)           => "PERCENT_RANK()"

    // TODO: case CumeDist()               => ???

    // ----------------------------------
    // Binary Expressions
    // ----------------------------------

    // arithmetic.scala

    case Add(Expression(left), Expression(right))       => op("+", left, right)
    case Subtract(Expression(left), Expression(right))  => op("-", left, right)
    case Multiply(Expression(left), Expression(right))  => op("*", left, right)
    case Divide(Expression(left), Expression(right))    => op("/", left, right)
    case Remainder(Expression(left), Expression(right)) => op("%", left, right)

    case Pmod(Expression(left), Expression(right)) =>
      block(block(block(left + "%" + right) + "+" + right) + "%" + right)

    // bitwiseExpressions.scala
    case BitwiseAnd(Expression(left), Expression(right)) => op("&", left, right)
    case BitwiseOr(Expression(left), Expression(right))  => op("|", left, right)
    case BitwiseXor(Expression(left), Expression(right)) => op("^", left, right)

    // datetimeExpressions.scala
    case DateAdd(Expression(startDate), Expression(days)) => f("ADDDATE", startDate, days)
    case DateSub(Expression(startDate), Expression(days)) => f("SUBDATE", startDate, days)
    case DateFormatClass(Expression(left), Expression(right), timeZoneId) =>
      f("DATE_FORMAT", left, right)

    // TODO: Support more datetime expressions
    // case _: ToUnixTimestamp  => None
    // case _: UnixTimestamp    => None
    // case _: FromUnixTime     => None
    // case _: NextDay          => None
    // case _: TimeAdd          => None
    // case _: FromUTCTimestamp => None
    // case _: TimeSub          => None
    // case _: AddMonths        => None
    // case _: MonthsBetween    => None
    // case _: ToUTCTimestamp   => None
    // case _: TruncDate        => None
    // case _: TruncTimestamp   => None
    // case _: DateDiff         => None

    // hash.scala
    case Sha2(Expression(left), Expression(right)) => f("SHA2", left, right)

    // mathExpressions.scala
    case Atan2(Expression(left), Expression(right))              => f("ATAN2", left, right)
    case Pow(Expression(left), Expression(right))                => f("POWER", left, right)
    case ShiftLeft(Expression(left), Expression(right))          => op("<<", left, right)
    case ShiftRight(Expression(left), Expression(right))         => op(">>", left, right)
    case ShiftRightUnsigned(Expression(left), Expression(right)) => op(">>", left, right)
    case Logarithm(Expression(left), Expression(right))          => f("LOG", left, right)
    case Round(Expression(child), Expression(scale))             => f("ROUND", child, scale)

    // TODO: case _: Hypot => None
    // TODO: case _: BRound => None

    // predicates.scala
    case And(Expression(left), Expression(right))                => op("AND", left, right)
    case Or(Expression(left), Expression(right))                 => op("OR", left, right)
    case EqualTo(Expression(left), Expression(right))            => op("=", left, right)
    case EqualNullSafe(Expression(left), Expression(right))      => op("<=>", left, right)
    case LessThan(Expression(left), Expression(right))           => op("<", left, right)
    case LessThanOrEqual(Expression(left), Expression(right))    => op("<=", left, right)
    case GreaterThan(Expression(left), Expression(right))        => op(">", left, right)
    case GreaterThanOrEqual(Expression(left), Expression(right)) => op(">=", left, right)

    case In(Expression(child), Expression(Some(elements))) =>
      op("IN", child, block(elements))

    case InSet(Expression(child), elements) =>
      op(
        "IN",
        child,
        block(elements.map(convertLiteral).reduce(_ + "," + _))
      )

    // regexpExpressions.scala
    case Like(Expression(left), Expression(right))  => op("LIKE", left, right)
    case RLike(Expression(left), Expression(right)) => op("RLIKE", left, right)

    // stringExpressions.scala
    case Contains(Expression(left), Expression(right)) =>
      op(">", f("INSTR", left, right), "0")
    case StartsWith(Expression(left), Expression(right)) =>
      op("LIKE", left, f("CONCAT", right, StringVar("%")))
    case EndsWith(Expression(left), Expression(right)) =>
      op("LIKE", left, f("CONCAT", StringVar("%"), right))
    case StringInstr(Expression(str), Expression(substr)) => f("INSTR", str, substr)
    case FormatNumber(Expression(x), Expression(d))       => f("FORMAT", x, d)
    case StringRepeat(Expression(child), Expression(times)) =>
      f("LPAD", StringVar(""), times + "*" + f("CHAR_LENGTH", child), child)

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
    case Conv(Expression(numExpr), Expression(fromBaseExpr), Expression(toBaseExpr)) =>
      f("CONV", numExpr, fromBaseExpr, toBaseExpr)

    // regexpExpressions.scala
    case RegExpReplace(Expression(subject), Expression(regexp), Expression(rep)) =>
      f("REGEXP_REPLACE", subject, regexp, rep)

    // TODO: case RegExpExtract(Expression(subject), Expression(regexp), Expression(idx)) => ???

    // stringExpressions.scala
    case StringReplace(Expression(srcExpr), Expression(searchExpr), Expression(replaceExpr)) =>
      f("REPLACE", srcExpr, searchExpr, replaceExpr)
    case SubstringIndex(Expression(strExpr), Expression(delimExpr), Expression(countExpr)) =>
      f("SUBSTRING_INDEX", strExpr, delimExpr, countExpr)
    case StringLocate(Expression(substr), Expression(str), Expression(start)) =>
      f("LOCATE", substr, str, start)
    case StringLPad(Expression(str), Expression(len), Expression(pad)) => f("LPAD", str, len, pad)
    case StringRPad(Expression(str), Expression(len), Expression(pad)) => f("RPAD", str, len, pad)
    case Substring(Expression(str), Expression(pos), Expression(len))  => f("SUBSTR", str, pos, len)

    // TODO: case StringTranslate(Expression(srcExpr), Expression(matchingExpr), Expression(replaceExpr)) => ???

    // ----------------------------------
    // Unary Expressions
    // ----------------------------------

    // arithmetic.scala
    case UnaryMinus(Expression(child))    => f("-", child)
    case UnaryPositive(Expression(child)) => f("+", child)
    case Abs(Expression(child))           => f("ABS", child)

    // bitwiseExpressions.scala
    case BitwiseNot(Expression(expr)) => f("~", expr)

    // Cast.scala
    case Cast(Expression(child), dataType, _) =>
      dataType match {
        case TimestampType => cast(child, "DATETIME(6)")
        case DateType      => cast(child, "DATE")

        case dt: DecimalType => makeDecimal(child, dt.precision, dt.scale)

        case StringType  => cast(child, "CHAR")
        case BinaryType  => cast(child, "BINARY")
        case ShortType   => op("!:>", child, "SMALLINT")
        case IntegerType => op("!:>", child, "INT")
        case LongType    => op("!:>", child, "BIGINT")
        case FloatType   => op("!:>", child, "DECIMAL(14, 7)")
        case DoubleType  => op("!:>", child, "DECIMAL(30, 15)")
        case BooleanType => op("!:>", child, "BOOL")

        // MemSQL doesn't know how to handle this cast, pass it through AS is
        case _ => child
      }

    // TODO: case UpCast(Expression(child), dataType, walkedTypePath) => ???

    // datetimeExpressions.scala
    case Hour(Expression(child), _)     => f("HOUR", child)
    case Minute(Expression(child), _)   => f("MINUTE", child)
    case Second(Expression(child), _)   => f("SECOND", child)
    case DayOfYear(Expression(child))   => f("DAYOFYEAR", child)
    case Year(Expression(child))        => f("YEAR", child)
    case Quarter(Expression(child))     => f("QUARTER", child)
    case Month(Expression(child))       => f("MONTH", child)
    case DayOfMonth(Expression(child))  => f("DAY", child)
    case DayOfWeek(Expression(child))   => f("DAYOFWEEK", child)
    case WeekOfYear(Expression(child))  => f("WEEK", child, "3")
    case LastDay(Expression(startDate)) => f("LAST_DAY", startDate)

    case ParseToDate(Expression(left), None, _)                     => f("DATE", left)
    case ParseToDate(Expression(left), Some(Expression(format)), _) => f("TO_DATE", left, format)

    case ParseToTimestamp(Expression(left), None, _) => f("TIMESTAMP", left)
    case ParseToTimestamp(Expression(left), Some(Expression(format)), _) =>
      f("TO_TIMESTAMP", left, format)

    // decimalExpressions.scala
    case MakeDecimal(Expression(child), p: Int, s: Int) => makeDecimal(child, p, s)

    // hash.scala
    case Md5(Expression(child))   => f("MD5", child)
    case Sha1(Expression(child))  => f("SHA1", child)
    case Crc32(Expression(child)) => f("CRC32", child)

    // mathExpressions.scala
    case Acos(Expression(child))      => f("ACOS", child)
    case Asin(Expression(child))      => f("ASIN", child)
    case Atan(Expression(child))      => f("ATAN", child)
    case Ceil(Expression(child))      => f("CEIL", child)
    case Cos(Expression(child))       => f("COS", child)
    case Exp(Expression(child))       => f("EXP", child)
    case Expm1(Expression(child))     => block(func("EXP", child) + "- 1")
    case Floor(Expression(child))     => f("FLOOR", child)
    case Log(Expression(child))       => f("LOG", child)
    case Log2(Expression(child))      => f("LOG2", child)
    case Log10(Expression(child))     => f("LOG10", child)
    case Log1p(Expression(child))     => f("LOG", child + "+ 1")
    case Signum(Expression(child))    => f("SIGN", child)
    case Sin(Expression(child))       => f("SIN", child)
    case Sqrt(Expression(child))      => f("SQRT", child)
    case Tan(Expression(child))       => f("TAN", child)
    case Cot(Expression(child))       => f("COT", child)
    case ToDegrees(Expression(child)) => f("DEGREES", child)
    case ToRadians(Expression(child)) => f("RADIANS", child)
    case Bin(Expression(child))       => f("BIN", child)
    case Hex(Expression(child))       => f("HEX", child)
    case Unhex(Expression(child))     => f("UNHEX", child)
    // tanh(x) = [exp(x) - exp(-x)] / [exp(x) + exp(-x)]
    case Tanh(Expression(child)) =>
      op("/",
         op("-", f("EXP", child), f("EXP", f("-", child))),
         op("+", f("EXP", child), f("EXP", f("-", child))))

    // TODO: case Cbrt(Expression(child))      => ???
    // TODO: case Cosh(Expression(child))      => ???
    // TODO: case Expm1(Expression(child))     => ???
    // TODO: case Factorial(Expression(child)) => ???
    // TODO: case Rint(Expression(child))      => ???
    // TODO: case Sinh(Expression(child))      => ???

    // nullExpressions.scala
    case IfNull(Expression(left), Expression(right), _) => f("COALESCE", left, right)
    case NullIf(Expression(left), Expression(right), _) => f("NULLIF", left, right)
    case Nvl(Expression(left), Expression(right), _)    => f("COALESCE", left, right)
    case IsNull(Expression(child))                      => block(child) + "IS NULL"
    case IsNotNull(Expression(child))                   => block(child) + "IS NOT NULL"

    case Nvl2(Expression(expr1), Expression(expr2), Expression(expr3), _) =>
      f("IF", expr1 + "IS NOT NULL", expr2, expr3)

    // predicates.scala
    case Not(Expression(child)) => block(Raw("NOT") + child)

    // randomExpressions.scala
    case Rand(Expression(child)) => f("RAND", child)
    // TODO: case Randn(Expression(child)) => ???

    // SortOrder.scala
    // in MemSQL, nulls always come first when direction = ascending
    case SortOrder(Expression(child), Ascending, NullsFirst, _) => block(child) + "ASC"
    // in MemSQL, nulls always come last when direction = descending
    case SortOrder(Expression(child), Descending, NullsLast, _) => block(child) + "DESC"

    // stringExpressions.scala
    case Upper(Expression(child)) => f("UPPER", child)
    case Lower(Expression(child)) => f("LOWER", child)

    case StringSpace(Expression(child)) => f("LPAD", "", child, StringVar(" "))

    case Right(Expression(str), Expression(len), _) => f("RIGHT", str, len)
    case Left(Expression(str), Expression(len), _)  => f("LEFT", str, len)
    case Length(Expression(child))                  => f("CHAR_LENGTH", child)
    case BitLength(Expression(child))               => block(func("LENGTH", child) + "* 8")
    case OctetLength(Expression(child))             => f("LENGTH", child)
    case Ascii(Expression(child))                   => f("ASCII", child)
    case Chr(Expression(child))                     => f("CHAR", child)
    case Base64(Expression(child))                  => f("TO_BASE64", child)
    case UnBase64(Expression(child))                => f("FROM_BASE64", child)

    // TODO: case InitCap(Expression(child)) => ???
    // TODO: case StringReverse(Expression(child)) => ???
    // TODO: case SoundEx(Expression(child)) => ???
  }
}
