package com.singlestore.spark

class MemsqlDefaultSource extends DefaultSource {

  override def shortName(): String = DefaultSource.MEMSQL_SOURCE_NAME_SHORT
}
