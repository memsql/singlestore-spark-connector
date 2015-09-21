package com.memsql.spark.connector.dataframe

case class MemSQLExtraColumn(name: String, colType: String, nullable: Boolean =true, defaultValue: Any = null, persisted: String = null)
{
  def toSQL: String = {
    val sql = new StringBuilder
    sql.append("`").append(name).append("` ")
    if (persisted != null) {
      sql.append("AS ").append(persisted).append(" PERSISTED ")
    }
    sql.append(colType).append(" ")
    if (!nullable) {
      sql.append("NOT NULL").append(" ")
    }
    if (persisted == null) {
      if (defaultValue == null && colType.toLowerCase() != "timestamp") {
        if (nullable) {
          sql.append("DEFAULT null")
        } else {
          sql.append("DEFAULT '0'")
        }
      } else if (defaultValue != null) {
        sql.append("DEFAULT ")
        defaultValue match {
          case dvs : String => sql.append("'").append(dvs).append("'")
          case dva : Any => sql.append(dva.toString)
        }
      }
    }
    sql.toString
  }
}
