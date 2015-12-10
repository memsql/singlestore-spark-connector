package com.memsql.spark.connector.sql

case class ColumnReference(name: String) {
  if (name.isEmpty) {
    throw new IllegalArgumentException("ColumnReference must have a non-empty column name.")
  }

  val quotedName: String = s"`$name`"
  val toSQL: String = quotedName
}

case class ColumnDefinition(name: String,
                            colType: String,
                            nullable: Boolean = true,
                            persisted: Option[String] = None,
                            defaultSQL: Option[String] = None
                           ) {

  if (name.isEmpty) {
    throw new IllegalArgumentException("ColumnDefinition must have a non-empty column name.")
  }
  if (colType.isEmpty) {
    throw new IllegalArgumentException("ColumnDefinition must have a non-empty colType.")
  }
  if (persisted.isDefined && defaultSQL.isDefined) {
    throw new IllegalArgumentException(
      "ColumnDefinition can not have both a persisted and defaultSQL expression.")
  }

  def reference: ColumnReference = ColumnReference(name)

  def nullExpression: String =
    if (nullable) { "NULL" } else { "NOT NULL" }

  def defaultExpression(defaultSQL: Option[String]): String =
    defaultSQL match {
      // If user specifies a default expression, add it
      case Some(d) => s"DEFAULT $d"

      // Otherwise add a default expression depending on nullability
      // Note: Timestamp columns are ignored so we don't mess with
      // the implicit NOW() DEFAULT expression
      case None => {
        val colTypeLower = colType.toLowerCase
        if (colTypeLower == "timestamp") {
          ""
        } else if (nullable) {
          "DEFAULT NULL"
        } else if (colTypeLower == "text" || colTypeLower == "blob") {
          "DEFAULT ''"
        } else {
          "DEFAULT '0'"
        }
      }
    }

  def toQueryFragment: QueryFragment = {
    val qf = QueryFragment()

    // Add the column name
    qf.quoted(name).space

    persisted match {
      case Some(p) => {
        // Add the persisted expression
        qf.raw("AS ").raw(p).raw(" PERSISTED ")

        // Add the column type
        qf.raw(colType)
      }
      case None => {
        // Add the column type
        qf.raw(colType).space

        // Add the null/not null specifier
        qf.raw(nullExpression).space

        // Maybe add default SQL expression
        qf.raw(defaultExpression(defaultSQL))
      }
    }
  }

  def toSQL: String = toQueryFragment.toSQL
}
