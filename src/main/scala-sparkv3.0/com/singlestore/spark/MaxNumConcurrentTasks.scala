package org.apache.spark.scheduler

import org.apache.spark.rdd.RDD

object MaxNumConcurrentTasks {
  def get(rdd: RDD[_]): Int = {
    rdd.sparkContext.maxNumConcurrentTasks()
  }
}
