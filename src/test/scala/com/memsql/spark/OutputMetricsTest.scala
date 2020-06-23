package com.memsql.spark

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.types.{IntegerType, StringType}

class OutputMetricsTest extends IntegrationSuiteBase {
  it("records written") {
    var outputWritten = 0L
    spark.sparkContext.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        val metrics = taskEnd.taskMetrics
        outputWritten += metrics.outputMetrics.recordsWritten
      }
    })

    val numRows = 100000
    val df1 = spark.createDF(
      List.range(0, numRows),
      List(("id", IntegerType, true))
    )

    df1.repartition(30)

    df1.write
      .format("memsql")
      .save("metricsInts")

    assert(outputWritten == numRows)
    outputWritten = 0

    val df2 = spark.createDF(
      List("st1", "", null),
      List(("st", StringType, true))
    )

    df2.write
      .format("memsql")
      .save("metricsStrings")

    assert(outputWritten == 3)
  }
}
