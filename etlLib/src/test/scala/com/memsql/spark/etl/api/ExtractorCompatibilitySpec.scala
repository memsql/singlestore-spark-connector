package com.memsql.spark.etl.api

import com.memsql.spark.etl.LocalSparkContext
import com.memsql.spark.etl.utils.{ByteUtils, PhaseLogger}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, Time, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import spray.json.JsNull

/*
  Ensure that changes to Extractor are backwards compatible.
 */
class ExtractorCompatibilitySpec extends FlatSpec with BeforeAndAfterEach with LocalSparkContext {
  var sqlContext: SQLContext = null
  var streamingContext: StreamingContext = null
  val config = UserExtractConfig("com.memsql.spark.etl.api.ExtractorCompatibility", JsNull)
  val batchInterval: Long = 5
  val batchTimestamp1: Long = 0
  val batchTimestamp2: Long = 1000
  val logger = new PhaseLogger {
    override protected val name: String = "logger"
    override protected val logger: Logger = Logger.getLogger("ExtractorCompatibilitySpec")
  }

  val testSequence = Seq(1, 2, 3, 4, 5) // scalastyle:ignore

  class TestSimpleByteArrayExtractor extends SimpleByteArrayExtractor {
    var initializeCalled = false
    var cleanupCalled = false
    var returnEmpty = false

    override def initialize(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long,
                            logger: PhaseLogger): Unit = {
      initializeCalled = true
    }

    override def cleanup(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long,
                         logger: PhaseLogger): Unit = {
      cleanupCalled = true
    }

    override def nextRDD(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long,
                         logger: PhaseLogger): Option[RDD[Array[Byte]]] = {
      val maybeRDD = returnEmpty match {
        case true => None
        case false => Some(sparkContext.parallelize(testSequence.map(byteUtils.intToBytes)))
      }

      returnEmpty = !returnEmpty

      maybeRDD
    }
  }

  class TestByteArrayExtractor extends ByteArrayExtractor {
    var initializeCalled = false
    var cleanupCalled = false
    var returnEmpty = false

    override def initialize(ssc: StreamingContext, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long,
                            logger: PhaseLogger): Unit = {
      initializeCalled = true
    }

    override def cleanup(ssc: StreamingContext, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long,
                         logger: PhaseLogger): Unit = {
      cleanupCalled = true
    }

    override def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchInterval: Long,
                         logger: PhaseLogger): InputDStream[Array[Byte]] = {
      new InputDStream[Array[Byte]](ssc) {
        override def stop(): Unit = {}

        override def start(): Unit = {}

        override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
          val maybeRDD = returnEmpty match {
            case true => None
            case false => Some(ssc.sparkContext.parallelize(testSequence.map(x => byteUtils.utf8StringToBytes(x.toString))))
          }

          returnEmpty = !returnEmpty

          maybeRDD
        }
      }
    }
  }

  class TestExtractor extends Extractor {
    var initializeCalled = false
    var cleanupCalled = false
    var returnEmpty = false

    override def initialize(ssc: StreamingContext, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long,
                            logger: PhaseLogger): Unit = {
      initializeCalled = true
    }

    override def cleanup(ssc: StreamingContext, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long,
                         logger: PhaseLogger): Unit = {
      cleanupCalled = true
    }

    override def next(ssc: StreamingContext, time: Long, sqlContext: SQLContext, extractConfig: PhaseConfig, batchInterval: Long,
                      logger: PhaseLogger): Option[DataFrame] = {
      val maybeDf = returnEmpty match {
        case true => None
        case false => {
          val rdd = ssc.sparkContext.parallelize(testSequence.map(x => Row(x > 3)))
          Some(sqlContext.createDataFrame(rdd, StructType(StructField("values", BooleanType, false) :: Nil)))
        }
      }

      returnEmpty = !returnEmpty

      maybeDf
    }
  }

  override protected def beforeEach(): Unit = {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("Test")
    sc = new SparkContext(sparkConfig)
    sqlContext = new SQLContext(sc)
    streamingContext = new StreamingContext(sc, Duration(1000)) // scalastyle:ignore
  }

  "SimpleByteArrayExtractor" should "initialize and cleanup correctly" in {
    val extractor = new TestSimpleByteArrayExtractor()
    assert(!extractor.initializeCalled)
    assert(!extractor.cleanupCalled)

    extractor.initialize(sc, config, batchInterval, logger)
    assert(extractor.initializeCalled)
    assert(!extractor.cleanupCalled)

    extractor.cleanup(sc, config, batchInterval, logger)
    assert(extractor.initializeCalled)
    assert(extractor.cleanupCalled)
  }

  it should "correctly return DataFrames" in {
    val extractor = new TestSimpleByteArrayExtractor()
    extractor.initialize(sc, config, batchInterval, logger)

    val maybeDf1 = extractor.next(streamingContext, batchTimestamp1, sqlContext, config, batchInterval, logger)
    assert(maybeDf1.nonEmpty)

    // check the results from the DataFrame
    val rows = maybeDf1.get.select("bytes").collect().map(_.toSeq.head.asInstanceOf[Array[Byte]])
    val values = rows.map(ByteUtils.bytesToInt).toSeq
    assert(values == testSequence)

    val maybeDf2 = extractor.next(streamingContext, batchTimestamp2, sqlContext, config, batchInterval, logger)
    assert(maybeDf2.isEmpty)

    extractor.cleanup(sc, config, batchInterval, logger)
  }

  "ByteArrayExtractor" should "initialize and cleanup correctly" in {
    val extractor = new TestByteArrayExtractor()
    assert(!extractor.initializeCalled)
    assert(!extractor.cleanupCalled)

    extractor.initialize(streamingContext, sqlContext, config, batchInterval, logger)
    assert(extractor.initializeCalled)
    assert(!extractor.cleanupCalled)

    extractor.cleanup(streamingContext, sqlContext, config, batchInterval, logger)
    assert(extractor.initializeCalled)
    assert(extractor.cleanupCalled)
  }

  it should "correctly return DataFrames" in {
    val extractor = new TestByteArrayExtractor()
    extractor.initialize(streamingContext, sqlContext, config, batchInterval, logger)

    val maybeDf1 = extractor.next(streamingContext, batchTimestamp1, sqlContext, config, batchInterval, logger)
    assert(maybeDf1.nonEmpty)

    // check the results from the DataFrame
    val rows = maybeDf1.get.select("bytes").collect().map(_.toSeq.head.asInstanceOf[Array[Byte]])
    val values = rows.map(ByteUtils.bytesToUTF8String).toSeq
    assert(values == testSequence.map(_.toString))

    val maybeDf2 = extractor.next(streamingContext, batchTimestamp2, sqlContext, config, batchInterval, logger)
    assert(maybeDf2.isEmpty)

    extractor.cleanup(streamingContext, sqlContext, config, batchInterval, logger)
  }

  "Extractor" should "initialize and cleanup correctly" in {
    val extractor = new TestExtractor()
    assert(!extractor.initializeCalled)
    assert(!extractor.cleanupCalled)

    extractor.initialize(streamingContext, sqlContext, config, batchInterval, logger)
    assert(extractor.initializeCalled)
    assert(!extractor.cleanupCalled)

    extractor.cleanup(streamingContext, sqlContext, config, batchInterval, logger)
    assert(extractor.initializeCalled)
    assert(extractor.cleanupCalled)
  }

  it should "correctly return DataFrames" in {
    val extractor = new TestExtractor()
    extractor.initialize(streamingContext, sqlContext, config, batchInterval, logger)

    val maybeDf1 = extractor.next(streamingContext, batchTimestamp1, sqlContext, config, batchInterval, logger)
    assert(maybeDf1.nonEmpty)

    // check the results from the DataFrame
    val values = maybeDf1.get.select("values").collect().map(_.toSeq.head.asInstanceOf[Boolean]).toSeq
    assert(values == testSequence.map(_ > 3))

    val maybeDf2 = extractor.next(streamingContext, batchTimestamp2, sqlContext, config, batchInterval, logger)
    assert(maybeDf2.isEmpty)

    extractor.cleanup(streamingContext, sqlContext, config, batchInterval, logger)
  }

  it should "only enable checkpointing if all methods are implemented" in {
    val extractor = new TestExtractor()
    assert(!extractor.usesCheckpointing())

    class CheckpointingExtractor extends TestExtractor {
      override def batchCheckpoint(): Option[Map[String, Any]] = None
      override def batchRetry(): Unit = {}
    }

    val checkpointingExtractor = new CheckpointingExtractor()
    assert(checkpointingExtractor.usesCheckpointing())

    class IncompleteCheckpointingExtractor extends TestExtractor {
      override def batchCheckpoint(): Option[Map[String, Any]] = None
    }

    val incompleteExtractor = new IncompleteCheckpointingExtractor()
    assert(!incompleteExtractor.usesCheckpointing())
  }
}
