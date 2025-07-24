package org.apache.spark.scheduler

import org.apache.spark.rdd.RDD

object MaxNumConcurrentTasks {
  def get(rdd: RDD[_]): Int = {
    val (_, resourceProfiles) =
      rdd.sparkContext.dagScheduler.getShuffleDependenciesAndResourceProfiles(rdd)
    val resourceProfile =
      rdd.sparkContext.dagScheduler.mergeResourceProfilesForStage(resourceProfiles)
    rdd.sparkContext.maxNumConcurrentTasks(resourceProfile)
  }
}
