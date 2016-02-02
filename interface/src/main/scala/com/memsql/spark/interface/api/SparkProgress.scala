package com.memsql.spark.interface.api

import com.memsql.spark.interface.util.BoundedMap
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{StageInfo, TaskInfo}
import org.apache.spark.{Success, TaskEndReason}


case class SparkProgressInfo(jobGroupId: String,
                             totalTasks: Int = 0,
                             tasksSucceeded: Int = 0,
                             tasksFailed: Int = 0,
                             bytesRead: Long = 0,
                             recordsRead: Long = 0)

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
                                         .copy(totalTasks = numTasks)
  }

  def updateTaskFinished(jobGroupId: String,
                         jobId: Int,
                         stageId: Int,
                         reason: TaskEndReason,
                         taskInfo: TaskInfo,
                         taskMetrics: TaskMetrics): Unit = {
    reason match {
      case Success => {
        for (inputMetrics <- taskMetrics.inputMetrics) {
          val progress = progressMap.getOrElseUpdate(jobGroupId, SparkProgressInfo(jobGroupId))

          val bytesRead = progress.bytesRead + inputMetrics.bytesRead
          val recordsRead = progress.recordsRead + inputMetrics.recordsRead
          val tasksSucceeded = progress.tasksSucceeded + 1

          progressMap(jobGroupId) = progressMap(jobGroupId).copy(bytesRead = bytesRead,
                                                                 recordsRead = recordsRead,
                                                                 tasksSucceeded = tasksSucceeded)
        }
      }
      case _ => {
        val progress = progressMap.getOrElseUpdate(jobGroupId, SparkProgressInfo(jobGroupId))
        val tasksFailed = progress.tasksFailed + 1
        progressMap(jobGroupId) = progressMap(jobGroupId).copy(tasksFailed = tasksFailed)
      }
    }
  }
}
