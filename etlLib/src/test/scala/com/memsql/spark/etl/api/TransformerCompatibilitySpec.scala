package com.memsql.spark.etl.api

import com.memsql.spark.etl.LocalSparkContext
import com.memsql.spark.etl.utils.{ByteUtils, PhaseLogger}
import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, Time, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import spray.json.JsNull

/*
  Ensure that changes to Transformer are backwards compatible.
 */
class TransformerCompatibilitySpec extends FlatSpec with BeforeAndAfterEach with LocalSparkContext {
  var sqlContext: SQLContext = null
  var streamingContext: StreamingContext = null
  val config = UserTransformConfig("com.memsql.spark.etl.api.TransformerCompatibility", JsNull)
  val batchInterval: Long = 5
  val batchTimestamp1: Long = 0
  val batchTimestamp2: Long = 1000
  val logger = new PhaseLogger {
    override protected val name: String = "logger"
    override protected val logger: Logger = Logger.getLogger("TransformerCompatibilitySpec")
  }

  val testSequence = Seq(1, 2, 3, 4, 5) // scalastyle:ignore

  class TestSimpleByteArrayTransformer extends SimpleByteArrayTransformer {
    var initializeCalled = false

    override def initialize(sqlContext: SQLContext, config: UserTransformConfig, logger: PhaseLogger): Unit = {
      initializeCalled = true
    }

    override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], config: UserTransformConfig,
                           logger: PhaseLogger): DataFrame = {
      val schema = StructType(StructField("value", IntegerType, false) :: Nil)
      sqlContext.createDataFrame(rdd.map(byteUtils.bytesToInt).map(2 * _).map(Row(_)), schema)
    }
  }

  class TestByteArrayTransformer extends ByteArrayTransformer {
    var initializeCalled = false

    override def initialize(sqlContext: SQLContext, config: PhaseConfig, logger: PhaseLogger): Unit = {
      initializeCalled = true
    }

    override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], config: PhaseConfig,
                           logger: PhaseLogger): DataFrame = {
      val schema = StructType(StructField("value", StringType, false) :: Nil)
      sqlContext.createDataFrame(rdd.map(byteUtils.bytesToUTF8String).map("foobar" + _).map(Row(_)), schema)
    }
  }

  class TestTransformer extends Transformer {
    var initializeCalled = false
    var cleanupCalled = false

    override def initialize(sqlContext: SQLContext, config: PhaseConfig, logger: PhaseLogger): Unit = {
      initializeCalled = true
    }

    override def cleanup(sqlContext: SQLContext, config: PhaseConfig, logger: PhaseLogger): Unit = {
      cleanupCalled = true
    }

    override def transform(sqlContext: SQLContext, df: DataFrame, config: PhaseConfig,
                           logger: PhaseLogger): DataFrame = {
      df.select(df("value") > 3 as 'threshold)
    }
  }

  override protected def beforeEach(): Unit = {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("Test")
    sc = new SparkContext(sparkConfig)
    sqlContext = new SQLContext(sc)
    streamingContext = new StreamingContext(sc, Duration(1000)) // scalastyle:ignore
  }

  "SimpleByteArrayTransformer" should "initialize correctly" in {
    val transformer = new TestSimpleByteArrayTransformer()
    assert(!transformer.initializeCalled)

    transformer.initialize(sqlContext, config, logger)
    assert(transformer.initializeCalled)
  }

  it should "correctly return DataFrames" in {
    val transformer = new TestSimpleByteArrayTransformer()
    transformer.initialize(sqlContext, config, logger)

    val rdd = sc.parallelize(testSequence.map(ByteUtils.intToBytes).map(Row(_)))
    val schema = StructType(StructField("bytes", BinaryType, false) :: Nil)
    val extractDf = sqlContext.createDataFrame(rdd, schema)

    val df = transformer.transform(sqlContext, extractDf, config, logger)

    // check the results from the DataFrame
    val rows = df.select("value").collect().map(_.toSeq.head.asInstanceOf[Int])
    val values = rows.toSeq
    assert(values == testSequence.map(2 * _))
  }

  "ByteArrayTransformer" should "initialize correctly" in {
    val transformer = new TestByteArrayTransformer()
    assert(!transformer.initializeCalled)

    transformer.initialize(sqlContext, config, logger)
    assert(transformer.initializeCalled)
  }

  it should "correctly return DataFrames" in {
    val transformer = new TestByteArrayTransformer()
    transformer.initialize(sqlContext, config, logger)

    val rdd = sc.parallelize(testSequence.map(_.toString).map(ByteUtils.utf8StringToBytes).map(Row(_)))
    val schema = StructType(StructField("bytes", BinaryType, false) :: Nil)
    val extractDf = sqlContext.createDataFrame(rdd, schema)

    val df = transformer.transform(sqlContext, extractDf, config, logger)

    // check the results from the DataFrame
    val rows = df.select("value").collect().map(_.toSeq.head.asInstanceOf[String])
    val values = rows.toSeq
    assert(values == testSequence.map("foobar" + _.toString))
  }

  "Transformer" should "initialize and cleanup correctly" in {
    val transformer = new TestTransformer()
    assert(!transformer.initializeCalled)
    assert(!transformer.cleanupCalled)

    transformer.initialize(sqlContext, config, logger)
    assert(transformer.initializeCalled)
    assert(!transformer.cleanupCalled)

    transformer.cleanup(sqlContext, config, logger)
    assert(transformer.initializeCalled)
    assert(transformer.cleanupCalled)
  }

  it should "correctly return DataFrames" in {
    val transformer = new TestTransformer()
    transformer.initialize(sqlContext, config, logger)

    val rdd = sc.parallelize(testSequence.map(Row(_)))
    val schema = StructType(StructField("value", IntegerType, false) :: Nil)
    val extractDf = sqlContext.createDataFrame(rdd, schema)

    val df = transformer.transform(sqlContext, extractDf, config, logger)

    // check the results from the DataFrame
    val rows = df.select("threshold").collect().map(_.toSeq.head.asInstanceOf[Boolean])
    val values = rows.toSeq
    assert(values == testSequence.map(_ > 3))
  }
}
