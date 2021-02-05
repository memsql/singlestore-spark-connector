package com.singlestore.spark

import java.util.concurrent.CountDownLatch
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.types.{IntegerType, StringType}

class OutputMetricsTest extends IntegrationSuiteBase {
  it("records written") {
    var outputWritten  = 0L
    var countDownLatch = new CountDownLatch(1);
    spark.sparkContext.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        val metrics = taskEnd.taskMetrics
        outputWritten += metrics.outputMetrics.recordsWritten
        countDownLatch.countDown()
      }
    })

    val numRows = 100000
    val df1 = spark.createDF(
      List.range(0, numRows),
      List(("id", IntegerType, true))
    )

    df1.repartition(30)

    df1.write
      .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
      .save("metricsInts")

    countDownLatch.await()
    assert(outputWritten == numRows)

    outputWritten = 0
    countDownLatch = new CountDownLatch(1);

    val df2 = spark.createDF(
      List("st1", "", null),
      List(("st", StringType, true))
    )

    df2.write
      .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
      .save("metricsStrings")

    countDownLatch.await()
    assert(outputWritten == 3)
  }
}
