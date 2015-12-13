package com.memsql.spark.interface.util.python

import java.util.{Map => JMap}
import com.memsql.spark.connector.MemSQLContext

import scala.collection.JavaConversions._
import com.memsql.spark.connector.sql.TableIdentifier
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.sql.memsql.SaveToMemSQLConf
import org.apache.spark.sql.memsql.SparkImplicits._

// Used by pystreamliner because Scala implicits don't play nicely with Py4J
object PystreamlinerUtils {
  def saveToMemSQL(memSQLContext: MemSQLContext,
                   df: DataFrame,
                   databaseName: String,
                   tableName: String,
                   saveModeString: String,
                   jParams: JMap[String, String]): Long = {
    val tableIdentifier = TableIdentifier(tableName, Option(databaseName))

    val saveMode = Option(saveModeString).map(SaveMode.valueOf)
    val params = Option(jParams).map(_.toMap).getOrElse(Map.empty)
    val saveToMemSQLConf: SaveToMemSQLConf = SaveToMemSQLConf(memSQLContext.memSQLConf, saveMode, params)

    df.saveToMemSQL(tableIdentifier, saveToMemSQLConf)
  }
}
