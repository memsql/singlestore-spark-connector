package com.memsql.spark.etl.api

import org.scalatest._
import spray.json._
import com.memsql.spark.etl.api._

class UserConfigSpec extends FlatSpec {
  val extractConfig = UserExtractConfig("com.test.Extract", JsObject(
    "a" -> JsObject(
      "foo" -> JsObject(
        "bar" -> JsArray(
          JsString("bar2"),
          JsNull,
          JsFalse,
          JsNumber(12),
          JsArray(JsNumber(1), JsString("true"), JsTrue, JsNull),
          JsNumber(12345l),
          JsNumber(1234.5f),
          JsNumber(1234567890123456789l)
        )
      )
    )
  ))

  val transformConfig = UserTransformConfig("com.test.Transform", JsObject(
    "a" -> JsObject(
      "foo" -> JsObject(),
      "bar" -> JsString("baz")
    ),
    "b" -> JsNull,
    "c" -> JsString("hello"),
    "false" -> JsArray(JsNumber(1), JsString("true"), JsTrue, JsNull)
  ))

  "UserConfig" should "parse JSON configs into a map" in {
    assert(extractConfig.getConfigAsMap == Map(
      "a" -> Map(
        "foo" -> Map(
          "bar" -> List(
            "bar2",
            null,
            false,
            12,
            List(1, "true", true, null),
            12345l,
            1234.5f,
            1234567890123456789l
          )
        )
      )
    ))

    assert(transformConfig.getConfigAsMap == Map(
      "a" -> Map(
        "foo" -> Map(),
        "bar" -> "baz"
      ),
      "b" -> null,
      "c" -> "hello",
      "false" -> List(1, "true", true, null)
    ))
  }

  it should "parse values from a nested config" in {
    assert(extractConfig.getConfigString("a", "foo", "bar", "0").get == "bar2")

    assert(extractConfig.getConfigString("a", "foo", "bar", "1").get == null)

    assert(extractConfig.getConfigBoolean("a", "foo", "bar", "2").get == false)

    assert(extractConfig.getConfigInt("a", "foo", "bar", "3").get == 12)
    assert(extractConfig.getConfigLong("a", "foo", "bar", "3").get == 12l)
    assert(extractConfig.getConfigFloat("a", "foo", "bar", "3").get == 12f)
    assert(extractConfig.getConfigDouble("a", "foo", "bar", "3").get == 12d)

    assert(extractConfig.getConfigInt("a", "foo", "bar", "4", "0").get == 1)
    assert(extractConfig.getConfigLong("a", "foo", "bar", "4", "0").get == 1l)
    assert(extractConfig.getConfigFloat("a", "foo", "bar", "4", "0").get == 1f)
    assert(extractConfig.getConfigDouble("a", "foo", "bar", "4", "0").get == 1d)

    assert(extractConfig.getConfigString("a", "foo", "bar", "4", "1").get == "true")

    assert(extractConfig.getConfigBoolean("a", "foo", "bar", "4", "2").get == true)

    assert(extractConfig.getConfigString("a", "foo", "bar", "4", "3").get == null)

    assert(extractConfig.getConfigInt("a", "foo", "bar", "5").get == 12345)
    assert(extractConfig.getConfigLong("a", "foo", "bar", "5").get == 12345l)
    assert(extractConfig.getConfigFloat("a", "foo", "bar", "5").get == 12345f)
    assert(extractConfig.getConfigDouble("a", "foo", "bar", "5").get == 12345d)

    assert(extractConfig.getConfigInt("a", "foo", "bar", "6").get == 1234)
    assert(extractConfig.getConfigLong("a", "foo", "bar", "6").get == 1234l)
    assert(extractConfig.getConfigFloat("a", "foo", "bar", "6").get == 1234.5f)
    assert(extractConfig.getConfigDouble("a", "foo", "bar", "6").get == 1234.5d)

    assert(extractConfig.getConfigInt("a", "foo", "bar", "7").get == (1234567890123456789l & Math.pow(2, 32).toLong - 1))
    assert(extractConfig.getConfigLong("a", "foo", "bar", "7").get == 1234567890123456789l)
    assert(extractConfig.getConfigFloat("a", "foo", "bar", "7").get == 1234567890123456789f)
    assert(extractConfig.getConfigDouble("a", "foo", "bar", "7").get == 1234567890123456789d)
  }

  it should "fail nicely if types don't match" in {
    assert(transformConfig.getConfigString("a", "foo").isEmpty)
    assert(transformConfig.getConfigString("false", "0").isEmpty)
    assert(transformConfig.getConfigString("false", "2").isEmpty)

    assert(transformConfig.getConfigBoolean("a", "foo").isEmpty)
    assert(transformConfig.getConfigBoolean("a", "bar").isEmpty)
    assert(transformConfig.getConfigBoolean("b").isEmpty)
    assert(transformConfig.getConfigBoolean("c").isEmpty)
    assert(transformConfig.getConfigBoolean("false", "0").isEmpty)
    assert(transformConfig.getConfigBoolean("false", "1").isEmpty)
    assert(transformConfig.getConfigBoolean("false", "3").isEmpty)

    assert(transformConfig.getConfigInt("a", "foo").isEmpty)
    assert(transformConfig.getConfigInt("a", "bar").isEmpty)
    assert(transformConfig.getConfigInt("b").isEmpty)
    assert(transformConfig.getConfigInt("c").isEmpty)
    assert(transformConfig.getConfigInt("false", "1").isEmpty)
    assert(transformConfig.getConfigInt("false", "2").isEmpty)
    assert(transformConfig.getConfigInt("false", "3").isEmpty)

    assert(transformConfig.getConfigLong("a", "foo").isEmpty)
    assert(transformConfig.getConfigLong("a", "bar").isEmpty)
    assert(transformConfig.getConfigLong("b").isEmpty)
    assert(transformConfig.getConfigLong("c").isEmpty)
    assert(transformConfig.getConfigLong("false", "1").isEmpty)
    assert(transformConfig.getConfigLong("false", "2").isEmpty)
    assert(transformConfig.getConfigLong("false", "3").isEmpty)

    assert(transformConfig.getConfigFloat("a", "foo").isEmpty)
    assert(transformConfig.getConfigFloat("a", "bar").isEmpty)
    //NOTE: JsNumber casts the null value to NaN
    assert(java.lang.Float.isNaN(transformConfig.getConfigFloat("b").get))
    assert(transformConfig.getConfigFloat("c").isEmpty)
    assert(transformConfig.getConfigFloat("false", "1").isEmpty)
    assert(transformConfig.getConfigFloat("false", "2").isEmpty)
    assert(java.lang.Float.isNaN(transformConfig.getConfigFloat("false", "3").get))

    assert(transformConfig.getConfigDouble("a", "foo").isEmpty)
    assert(transformConfig.getConfigDouble("a", "bar").isEmpty)
    assert(java.lang.Double.isNaN(transformConfig.getConfigDouble("b").get))
    assert(transformConfig.getConfigDouble("c").isEmpty)
    assert(transformConfig.getConfigDouble("false", "1").isEmpty)
    assert(transformConfig.getConfigDouble("false", "2").isEmpty)
    assert(java.lang.Double.isNaN(transformConfig.getConfigDouble("false", "3").get))
  }

  it should "fail nicely if path doesnt exist" in {
    assert(extractConfig.getConfigString("foo", "bar", "baz", "qux", "12").isEmpty)
    assert(extractConfig.getConfigBoolean("foo", "bar", "baz", "qux", "12").isEmpty)
    assert(extractConfig.getConfigInt("foo", "bar", "baz", "qux", "12").isEmpty)
    assert(extractConfig.getConfigLong("foo", "bar", "baz", "qux", "12").isEmpty)
    assert(extractConfig.getConfigFloat("foo", "bar", "baz", "qux", "12").isEmpty)
    assert(extractConfig.getConfigDouble("foo", "bar", "baz", "qux", "12").isEmpty)

    assert(extractConfig.getConfigString("a", "foo", "bar", "baz").isEmpty)
    assert(extractConfig.getConfigBoolean("a", "foo", "bar", "baz").isEmpty)
    assert(extractConfig.getConfigInt("a", "foo", "bar", "baz").isEmpty)
    assert(extractConfig.getConfigLong("a", "foo", "bar", "baz").isEmpty)
    assert(extractConfig.getConfigFloat("a", "foo", "bar", "baz").isEmpty)
    assert(extractConfig.getConfigDouble("a", "foo", "bar", "baz").isEmpty)

    assert(extractConfig.getConfigString("a", "foo", "bar", "baz").isEmpty)
    assert(extractConfig.getConfigBoolean("a", "foo", "bar", "baz").isEmpty)
    assert(extractConfig.getConfigInt("a", "foo", "bar", "baz").isEmpty)
    assert(extractConfig.getConfigLong("a", "foo", "bar", "baz").isEmpty)
    assert(extractConfig.getConfigFloat("a", "foo", "bar", "baz").isEmpty)
    assert(extractConfig.getConfigDouble("a", "foo", "bar", "baz").isEmpty)

    assert(extractConfig.getConfigString("false", "3", "a").isEmpty)
    assert(extractConfig.getConfigBoolean("false", "3", "a").isEmpty)
    assert(extractConfig.getConfigInt("false", "3", "a").isEmpty)
    assert(extractConfig.getConfigLong("false", "3", "a").isEmpty)
    assert(extractConfig.getConfigFloat("false", "3", "a").isEmpty)
    assert(extractConfig.getConfigDouble("false", "3", "a").isEmpty)
  }
}
