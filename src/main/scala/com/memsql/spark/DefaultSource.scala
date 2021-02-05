package com.memsql.spark

import com.singlestore.spark

class DefaultSource extends spark.DefaultSource {

  override def shortName(): String = spark.DefaultSource.MEMSQL_SOURCE_NAME_SHORT
}
