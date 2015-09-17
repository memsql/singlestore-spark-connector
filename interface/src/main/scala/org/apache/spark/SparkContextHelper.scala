package org.apache.spark

object SparkContextHelper {
  // This object allows us to access private members of SparkContext (e.g the
  // .ui member).
  def getUIBoundPort(sc: SparkContext): Int = {
    sc.ui.map(_.boundPort).getOrElse(-1)
  }
}
