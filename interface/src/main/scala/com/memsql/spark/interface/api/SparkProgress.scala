package com.memsql.spark.interface.api

import com.memsql.spark.interface.util.BoundedMap
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{StageInfo, TaskInfo}
import org.apache.spark.{Success, TaskEndReason}


case class SparkProgressInfo(job_group_id: String,
                             total_tasks: Int = 0,
                             tasks_succeeded: Int = 0,
                             tasks_failed: Int = 0,
                             bytes_read: Long = 0,
                             records_read: Long = 0)

class SparkProgress {
  val MAX_PROGRESS_MAP_SIZE = 1000
  private val progressMap = new BoundedMap[String, SparkProgressInfo](MAX_PROGRESS_MAP_SIZE)

  def get(jobGroupId: String): Option[SparkProgressInfo] = progressMap.get(jobGroupId)

  def updateJobStarted(jobGroupId: String,
                       jobId: Int,
                       stageInfos: Seq[StageInfo]): Unit = {

    // Potential underestimate; see https://github.com/apache/spark/pull/3009
    val numTasks = {
      val allStages = stageInfos
      val missingStages = allStages.filter(_.completionTime.isEmpty)
      missingStages.map(_.numTasks).sum
    }

    progressMap(jobGroupId) = progressMap.getOrElseUpdate(jobGroupId, SparkProgressInfo(jobGroupId))
                                         .copy(total_tasks = numTasks)
  }

  def updateTaskFinished(jobGroupId: String,
                         jobId: Int,
                         stageId: Int,
                         reason: TaskEndReason,
                         taskInfo: TaskInfo,
                         taskMetrics: TaskMetrics): Unit = {
    reason match {
      case Success => {
        val progress = progressMap.getOrElseUpdate(jobGroupId, SparkProgressInfo(jobGroupId))
        val tasks_succeeded = progress.tasks_succeeded + 1
        progressMap(jobGroupId) = progressMap(jobGroupId).copy(tasks_succeeded = tasks_succeeded)

        for (inputMetrics <- taskMetrics.inputMetrics) {
          val bytes_read = progress.bytes_read + inputMetrics.bytesRead
          val records_read = progress.records_read + inputMetrics.recordsRead
          progressMap(jobGroupId) = progressMap(jobGroupId).copy(bytes_read = bytes_read,
                                                                 records_read = records_read)
        }
      }
      case _ => {
        val progress = progressMap.getOrElseUpdate(jobGroupId, SparkProgressInfo(jobGroupId))
        val tasks_failed = progress.tasks_failed + 1
        progressMap(jobGroupId) = progressMap(jobGroupId).copy(tasks_failed = tasks_failed)
      }
    }
  }
}
