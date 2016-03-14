package com.memsql.spark.interface

import com.memsql.spark.interface.api.SparkProgress
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.ui.jobs.JobProgressListener

import scala.collection.mutable.HashMap

class SparkProgressListener(conf: SparkConf) extends JobProgressListener(conf) {

  val stageIdToLastStartedJobId = new HashMap[StageId, JobId]
  val jobIdToJobGroup = new HashMap[JobId, JobGroupId]

  val sparkProgress = new SparkProgress

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
    super.onJobStart(jobStart)

    for (stageId <- jobStart.stageIds) {
      stageIdToLastStartedJobId(stageId) = jobStart.jobId
    }

    for (props <- Option(jobStart.properties);
         jobGroupId <- Option(props.getProperty("spark.jobGroup.id")))
    {
      jobIdToJobGroup(jobStart.jobId) = jobGroupId
      sparkProgress.updateJobStarted(jobGroupId, jobStart.jobId, jobStart.stageInfos)
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = synchronized {
    super.onJobEnd(jobEnd)

    stageIdToLastStartedJobId.retain((k, v) => {
      v != jobEnd.jobId
    })
    jobIdToJobGroup -= jobEnd.jobId
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    super.onTaskEnd(taskEnd)

    val stageId = taskEnd.stageId
    val maybeJobId = stageIdToLastStartedJobId.get(stageId)
    val maybeJobGroupId: Option[JobGroupId] = maybeJobId.flatMap(jobIdToJobGroup.get)

    for (jobId <- maybeJobId; jobGroupId <- maybeJobGroupId) {
      val reason = taskEnd.reason
      val taskInfo = taskEnd.taskInfo
      val taskMetrics = taskEnd.taskMetrics
      sparkProgress.updateTaskFinished(jobGroupId, maybeJobId.get, stageId,
                                       reason, taskInfo, taskMetrics)
    }
  }
}
