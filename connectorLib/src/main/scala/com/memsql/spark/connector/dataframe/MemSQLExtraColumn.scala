package com.memsql.spark.connector.dataframe

/**
 * Representation for an extra column in MemSQL.
 *
 * @param name Name of the column.
 * @param colType SQL type for this column. See
 *                <a href="http://docs.memsql.com/latest/ref/CREATE_TABLE/#create-table">MemSQL CREATE TABLE docs</a>
 *                for the full list of supported types.
 * @param nullable Allow `NULL` values for this column.
 * @param defaultSql A SQL expression to put in the DEFAULT definition of this
 *                   column.
 * @param persisted Persist this column if it is computed.
 */
case class MemSQLExtraColumn(name: String, colType: String, nullable: Boolean = true, defaultSql: String = null, persisted: String = null)
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
      if (defaultSql == null && colType.toLowerCase() != "timestamp") {
        if (nullable) {
          sql.append("DEFAULT null")
        } else {
          sql.append("DEFAULT '0'")
        }
      } else if (defaultSql != null) {
        sql.append("DEFAULT ")
        sql.append(defaultSql)
      }
    }
    sql.toString
  }
}
