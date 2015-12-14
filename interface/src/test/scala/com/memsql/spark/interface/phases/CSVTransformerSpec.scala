// scalastyle:off magic.number
package com.memsql.spark.interface.phases

import java.sql.Timestamp

import com.memsql.spark.connector.dataframe._
import com.memsql.spark.etl.LocalSparkContext
import com.memsql.spark.etl.utils.ByteUtils._
import com.memsql.spark.interface.TestKitSpec
import com.memsql.spark.interface.util.PipelineLogger
import com.memsql.spark.phases.{CSVTransformerConfig, CSVTransformer, CSVTransformerException}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.{Duration, StreamingContext}
import spray.json._

class CSVTransformerSpec extends TestKitSpec("CSVTransformerSpec") with LocalSparkContext{
  val DURATION_CONST = 5000
  val transformer = new CSVTransformer
  var sqlContext: SQLContext = null
  var streamingContext: StreamingContext = null
  var logger: PipelineLogger = null

  override def beforeEach(): Unit = {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("Test")
    sc = new SparkContext(sparkConfig)
    sqlContext = new SQLContext(sc)
    streamingContext = new StreamingContext(sc, new Duration(DURATION_CONST))
    logger = new PipelineLogger(s"Pipeline p1 transform", false)
  }

  "CSVTransformer" should {
    "work on DataFrame with String column" in {
      val config = CSVTransformerConfig(
        delimiter = Some(','),
        escape = Some("\\"),
        quote = Some('\''),
        null_string = Some("NULL"),
        columns = """[{"name": "id", "column_type": "string"}, {"name": "name", "column_type": "string"}]""".parseJson
      )

      val schema = StructType(StructField("string", StringType, false) :: Nil)
      val sampleData = List(
        "1,hello",
        "2,world",
        "3,foo",
        "4,bar"
      )
      val rowRDD = sqlContext.sparkContext.parallelize(sampleData).map(Row(_))
      val dfIn = sqlContext.createDataFrame(rowRDD, schema)

      val df = transformer.transform(sqlContext, dfIn, config, logger)
      val schemaOut = StructType(
        StructField("id", StringType, true) ::
        StructField("name", StringType, true) :: Nil)

      assert(df.schema == schemaOut)
      assert(df.first == Row("1", "hello"))
      assert(df.count == 4)
    }

    "work on DataFrame with Binary column" in {
      val config = CSVTransformerConfig(
        delimiter = Some(','),
        escape = Some("\\"),
        quote = Some('\''),
        null_string = Some("NULL"),
        columns = """[{"name": "id", "column_type": "string"}, {"name": "name", "column_type": "string"}]""".parseJson
      )

      val schema = StructType(StructField("bytes", BinaryType, false) :: Nil)
      val sampleData = List(
        "1,hello",
        "2,world",
        "3,foo",
        "4,bar"
      ).map(utf8StringToBytes)
      val rowRDD = sqlContext.sparkContext.parallelize(sampleData).map(Row(_))
      val dfIn = sqlContext.createDataFrame(rowRDD, schema)

      val df = transformer.transform(sqlContext, dfIn, config, logger)
      val schemaOut = StructType(
        StructField("id", StringType, true) ::
        StructField("name", StringType, true) :: Nil)

      assert(df.schema == schemaOut)
      assert(df.first == Row("1", "hello"))
      assert(df.count == 4)
    }

    "NOT work on DataFrame with column different than String or Binary" in {
      val config = CSVTransformerConfig(
        delimiter = Some(','),
        escape = Some("\\"),
        quote = Some('\''),
        null_string = Some("NULL"),
        columns = """[{"name": "id", "column_type": "string"}, {"name": "name", "column_type": "string"}]""".parseJson
      )

      val schema = StructType(StructField("data", IntegerType, false) :: Nil)
      val sampleData = List(
        "1,hello",
        "2,world",
        "3,foo",
        "4,bar"
      ).map(utf8StringToBytes)
      val rowRDD = sqlContext.sparkContext.parallelize(sampleData).map(Row(_))
      val dfIn = sqlContext.createDataFrame(rowRDD, schema)

      val e = intercept[IllegalArgumentException] {
        transformer.transform(sqlContext, dfIn, config, logger)
      }
      assert(e.getMessage() == "The first column of the input DataFrame should be either StringType or BinaryType")
    }

    "allow the empty character for escape" in {
      val config = CSVTransformerConfig(
        delimiter = Some('/'),
        escape = Some(""),
        quote = Some('\''),
        null_string = Some("NULL"),
        columns = """[{"name": "sample_column", "column_type": "string"}]""".parseJson
      )

      val schema = StructType(StructField("string", StringType, false) :: Nil)
      val sampleData = List(
        "test\\default"
      )
      val rowRDD = sqlContext.sparkContext.parallelize(sampleData).map(Row(_))
      val dfIn = sqlContext.createDataFrame(rowRDD, schema)

      try {
        val df = transformer.transform(sqlContext, dfIn, config, logger)
        assert(df.rdd.collect()(0)(0).toString == "test\\default", "Input not preserved");
      } catch {
        case e: CSVTransformerException => fail("CSVTransformer exception")
        case e: Throwable => fail(s"Unexpected response $e")
      }
    }

    "fail on an invalid column json" in {
      val config = CSVTransformerConfig(
        delimiter = Some('/'),
        escape = Some("\\"),
        quote = Some('\''),
        null_string = Some("NULL"),
        columns = """{"name": "sample_column", "column_type": "string"}""".parseJson
      )

      val schema = StructType(StructField("string", StringType, false) :: Nil)
      val sampleData = List(
        "test\\default"
      )
      val rowRDD = sqlContext.sparkContext.parallelize(sampleData).map(Row(_))
      val dfIn = sqlContext.createDataFrame(rowRDD, schema)

      try {
        transformer.transform(sqlContext, dfIn, config, logger)
      } catch {
        case e: Throwable => assert(e.isInstanceOf[DeserializationException])
      }
    }

    "fail if the escape is longer than 1 character" in {
      val config = CSVTransformerConfig(
        delimiter = Some('/'),
        escape = Some("\\\\"),
        quote = Some('\''),
        null_string = Some("NULL"),
        columns = """[{"name": "sample_column", "column_type": "string"}]""".parseJson
      )

      val schema = StructType(StructField("string", StringType, false) :: Nil)
      val sampleData = List(
        "test\\default"
      )
      val rowRDD = sqlContext.sparkContext.parallelize(sampleData).map(Row(_))
      val dfIn = sqlContext.createDataFrame(rowRDD, schema)

      try {
        transformer.transform(sqlContext, dfIn, config, logger)
      } catch {
        case e: Throwable => assert(e.isInstanceOf[CSVTransformerException])
      }
    }


    "convert values to the expected type" in {
      val config = CSVTransformerConfig(
        delimiter = Some(','),
        escape = Some("\\"),
        quote = Some('\''),
        null_string = Some("NULL"),
        columns = """[
          { "name": "short", "column_type": "short" },
          { "name": "integer", "column_type": "integer" },
          { "name": "bigint", "column_type": "bigint" },
          { "name": "bigint_unsigned", "column_type": "bigint unsigned" },
          { "name": "float", "column_type": "float" },
          { "name": "double", "column_type": "double" },
          { "name": "boolean", "column_type": "boolean" },
          { "name": "timestamp", "column_type": "timestamp" },
          { "name": "byte", "column_type": "byte" },
          { "name": "binary", "column_type": "binary" },

          { "name": "string", "column_type": "string" },
          { "name": "json", "column_type": "json" },
          { "name": "geography", "column_type": "geography" },
          { "name": "geography_point", "column_type": "geographypoint" },
          { "name": "notype" }
        ]""".parseJson
      )

      val schema = StructType(StructField("string", StringType, false) :: Nil)
      val sampleData = List(
        "256", "2048", "10000000", "999999999", "1.2", "4.7", "false", "2014-02-02T12:25:36", "1", "2",
        "string", "[]", "geo1", "geopoint1", "string2"
      )
      val rowRDD = sqlContext.sparkContext.parallelize(List(sampleData.mkString(","))).map(Row(_))
      val dfIn = sqlContext.createDataFrame(rowRDD, schema)

      val transformedDf = transformer.transform(sqlContext, dfIn, config, logger)
      val transformedValues = transformedDf.first().toSeq.toList.map {
        // convert arrays for comparison
        case arr: Array[_] => arr.toSeq.toList
        case default => default
      }
      val expectedValues = List(
        256.toShort, 2048, 10000000.toLong, new BigIntUnsignedValue(999999999), 1.2f, 4.7, false,
        new Timestamp(114, 1, 2, 12, 25, 36, 0), 1.toByte, List('2'.toByte),
        "string", new JsonValue("[]"), new GeographyValue("geo1"), new GeographyPointValue("geopoint1"), "string2"
      )

      assert(transformedValues == expectedValues)
    }
  }
}
