package com.memsql.spark.connector.dataframe

case class MemSQLExtraColumn(name: String, colType: String, nullable: Boolean =true, defaultValue: String = null, persisted: String = null)
{
  def toSQL: String = {
    var sql = new StringBuilder
    sql.append(name).append(" ")
    if (persisted != null) {
      sql.append("AS ").append(persisted).append(" PERSISTED ")
    }
    sql.append(colType).append(" ")
    if (!nullable) {
      sql.append("NOT NULL").append(" ")
    }
    if (persisted == null) {
      if (defaultValue == null) {
        if (nullable) {
          sql.append("DEFAULT null")
        } else {
          sql.append("DEFAULT '0'")
        }
      } else {
        sql.append("DEFAULT ").append(defaultValue)
      }
    }
    sql.toString
  }
}
