package com.singlestore.spark

import org.apache.spark.sql.types.{
  CalendarIntervalType,
  DataType,
  DayTimeIntervalType,
  YearMonthIntervalType
}

object VersionSpecificUtil {
  def isIntervalType(d: DataType): Boolean =
    d.isInstanceOf[CalendarIntervalType] || d.isInstanceOf[DayTimeIntervalType] || d
      .isInstanceOf[YearMonthIntervalType]
}
