package com.singlestore.spark

import java.util.concurrent.CountDownLatch
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.types.{IntegerType, StringType}

class OutputMetricsTest extends IntegrationSuiteBase {
  it("records written") {
    var outputWritten                  = 0L
    var countDownLatch: CountDownLatch = null
    spark.sparkContext.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        if (taskEnd.taskType == "ResultTask") {
          outputWritten.synchronized({
            val metrics = taskEnd.taskMetrics
            outputWritten += metrics.outputMetrics.recordsWritten
            countDownLatch.countDown()
          })
        }
      }
    })

    val numRows = 100000
    var df1 = spark.createDF(
      List.range(0, numRows),
      List(("id", IntegerType, true))
    )

    var numPartitions = 30
    countDownLatch = new CountDownLatch(numPartitions)
    df1 = df1.repartition(numPartitions)

    df1.write
      .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
      .save("metricsInts")

    countDownLatch.await()
    assert(outputWritten == numRows)

    var df2 = spark.createDF(
      List("st1", "", null),
      List(("st", StringType, true))
    )

    outputWritten = 0
    numPartitions = 1
    countDownLatch = new CountDownLatch(numPartitions)
    df2 = df2.repartition(numPartitions)

    df2.write
      .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
      .save("metricsStrings")
    countDownLatch.await()
    assert(outputWritten == 3)
  }
}
