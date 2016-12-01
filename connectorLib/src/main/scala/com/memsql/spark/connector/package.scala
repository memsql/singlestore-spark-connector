package com.memsql.spark

import com.memsql.spark.connector.dataframe.TypeConversions
import com.memsql.spark.connector.sql.ColumnDefinition
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

package object connector {

  /* MemSQL functions on DataFrames */
  implicit def dataFrameFunctions(df: DataFrame): DataFrameFunctions = new DataFrameFunctions(df)

  implicit def sparkContextFunctions(sc: SparkContext): SparkContextFunctions = new SparkContextFunctions(sc)

  class SparkContextFunctions(sc: SparkContext) extends Serializable {
    var memSQLConf: MemSQLConf = MemSQLConf(sc.getConf)
    def setDatabase(dbName: String): Unit = memSQLConf = memSQLConf.copy(defaultDBName = dbName)
    def getDatabase: String = memSQLConf.defaultDBName

    def getMemSQLCluster: MemSQLCluster = MemSQLCluster(memSQLConf)
  }

  implicit def sparkSessionFunctions(sparkSession: SparkSession): SparkSessionFunctions = new SparkSessionFunctions(sparkSession)

  class SparkSessionFunctions(sparkSession: SparkSession) extends Serializable {
    var memSQLConf: MemSQLConf = MemSQLConf(sparkSession.sparkContext.getConf)
  }

  implicit def schemaFunctions(schema: StructType): SchemaFunctions = new SchemaFunctions(schema)

  class SchemaFunctions(schema: StructType) extends Serializable {
    def toMemSQLColumns: Seq[ColumnDefinition] = {
      schema.map(s => {
        ColumnDefinition(
          s.name,
          TypeConversions.DataFrameTypeToMemSQLTypeString(s.dataType),
          s.nullable
        )
      })
    }
  }
}

