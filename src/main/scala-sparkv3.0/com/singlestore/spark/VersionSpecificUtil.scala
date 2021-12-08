package com.singlestore.spark

import org.apache.spark.sql.types.{CalendarIntervalType, DataType}

object VersionSpecificUtil {
  def isIntervalType(d: DataType): Boolean =
    d.isInstanceOf[CalendarIntervalType]
}
