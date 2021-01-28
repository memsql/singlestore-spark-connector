package org.apache.spark.metrics.source

import org.apache.spark.TaskContext

object MetricsHandler {
  def setRecordsWritten(r: Long): Unit = {
    TaskContext.get().taskMetrics().outputMetrics.setRecordsWritten(r)
  }
}
