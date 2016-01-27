//scalastyle:off magic.number
package com.memsql.spark.interface.phases

import com.memsql.spark.etl.LocalSparkContext
import com.memsql.spark.interface.TestKitSpec
import com.memsql.spark.interface.util.PipelineLogger
import com.memsql.spark.phases.{S3ExtractConfig, S3ExtractTaskConfig, S3ExtractException, S3Extractor}
import org.apache.hadoop.fs.s3.S3Exception
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.Matchers

import scala.concurrent.duration._
import scala.concurrent.{Await, future}

class S3ExtractorSpec extends TestKitSpec("S3ExtractorSpec") with LocalSparkContext with Matchers {
  val DURATION_CONST = 5000
  val extractor = new S3Extractor
  var ssc: StreamingContext = null
  var sqlContext: SQLContext = null
  var logger: PipelineLogger = null

  override def beforeEach(): Unit = {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("Test")
    sc = new SparkContext(sparkConfig)
    ssc = new StreamingContext(sc, new Duration(DURATION_CONST))
    sqlContext = new SQLContext(sc)
    logger = new PipelineLogger(s"S3Extractor Test", false)
  }

  // Spark will automatically check the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
  // environment variables, even if they're specified in the hadoopConfiguration.
  // We want to test with and without correct credentials, so we put them under
  // these names instead.
  val awsAccessKeyID = sys.env("AWS_ACCESS_KEY_ID_TEST")
  val awsSecretAccessKey = sys.env("AWS_SECRET_ACCESS_KEY_TEST")

  val bucket = "loader-tests"

  def testConfig(key: String, max_records: Option[Int] = None): S3ExtractConfig = {
    val task_config = S3ExtractTaskConfig(key = key)
    S3ExtractConfig(awsAccessKeyID, awsSecretAccessKey, bucket, task_config, max_records)
  }

  def stepExtractor(config: S3ExtractConfig): Option[DataFrame] = {
    try {
      extractor.next(ssc, System.currentTimeMillis, sqlContext, config, 0, logger)
    } finally {
      extractor.cleanup(ssc, sqlContext, config, 0, logger)
    }
  }

  "S3Extractor" should {
    "pull a one-line file from S3" in {
      val config = testConfig("one_line/file1")
      val dfOption = stepExtractor(config)

      dfOption match {
        case Some(df) => {
          val rows = df.collect
          assert(rows.length == 1)

          val row = rows(0)
          assert(row.length == 1)

          val line = row.getString(0)
          assert(line =="10, hello, world")
        }
        case None => fail("extractor did not return a dataframe")
      }
    }

    "strip a single-slash prefix" in {
      val config = testConfig("/one_line/file1")
      val dfOption = stepExtractor(config)

      dfOption match {
        case Some(df) => {
          val rows = df.collect
          val row = rows(0)
          val line = row.getString(0)
          assert(line =="10, hello, world")
        }
        case None => fail("extractor did not return a dataframe")
      }
    }

    "fail on bad credentials" in {
      val config = S3ExtractConfig("foo", "bar", bucket, S3ExtractTaskConfig("one_line/file1"), None)
      val dfOption = stepExtractor(config)

      dfOption match {
        case Some(df) => {
          an[S3Exception] should be thrownBy {
            df.collect
          }
        }
        case None => fail("extractor did not return a dataframe")
      }
    }

    "pull a medium-sized file from S3" in {
      val config = testConfig("big_files/file1")
      val dfOption = stepExtractor(config)

      dfOption match {
        case Some(df) => {
          assert(df.count == 300000)
        }
        case None => fail("extractor did not return a dataframe")
      }
    }

    "truncate files if necessary" in {
      val config = testConfig("big_files/file1", max_records = Some(10))
      val dfOption = stepExtractor(config)

      dfOption match {
        case Some(df) => {
          assert(df.count == 10)
        }
        case None => fail("extractor did not return a dataframe")
      }
    }

    "fail on a non-existent bucket" in {
      val config = S3ExtractConfig(awsAccessKeyID, awsSecretAccessKey, "this_bucket_does_not_exist", S3ExtractTaskConfig("foo"), None)

      an[S3ExtractException] should be thrownBy {
        extractor.next(ssc, System.currentTimeMillis, sqlContext, config, 0, logger)
      }
    }

    "fail on a non-existent file" in {
      val config = testConfig("this/path/does/not/exist")
      val dfOption = stepExtractor(config)

      dfOption match {
        case Some(df) => {
          an[InvalidInputException] should be thrownBy {
            df.collect
          }
        }
        case None => fail("extractor did not return a dataframe")
      }
    }

    "properly handle two threads with different credentials" in {
      import scala.concurrent.ExecutionContext.Implicits.global

      val config1 = testConfig("big_files/file2")
      val config2 = S3ExtractConfig("foo", "bar", bucket, S3ExtractTaskConfig("one_line/file1"), None)

      val dfOption1 = stepExtractor(config1)
      val dfOption2 = stepExtractor(config2)

      // Start a long-running extractor
      val dfFuture1 = future {
        dfOption1 match {
          case Some(df) => {
            assert(df.count == 300000)
          }
          case None => fail("extractor 1 did not return a dataframe")
        }
      }

      Thread.sleep(100) // scalastyle:ignore

      // The other extractor should fail
      dfOption2 match {
        case Some(df) => {
          an[S3Exception] should be thrownBy {
            df.collect
          }
        }
        case None => fail("extractor 2 did not return a dataframe")
      }

      // The first extractor should be unaffected
      Await.result(dfFuture1, 15.seconds)
    }
  }
}
