package com.memsql.spark.connector.sql

abstract class MemSQLColumn {
  def name: String
  def colType: String
  def nullable: Boolean

  def toQueryFragment: QueryFragment
  def toSQL: String = toQueryFragment.sql.toString

  def quotedName: String = s"`$name`"

  def nullExpression: String =
    if (nullable) { "NULL" } else { "NOT NULL" }

  def defaultExpression(defaultSQL: Option[String]): String =
    defaultSQL match {
      // If user specifies a default expression, add it
      case Some(d) => s"DEFAULT $d"

      // Otherwise add a default expression depending on nullability
      // Note: Timestamp columns are locked out so we don't mess with
      // the implicit NOW() DEFAULT expression
      case None if colType.toLowerCase() != "timestamp" =>
        if (nullable) { "DEFAULT NULL" }
        else { "DEFAULT '0'" }

      case _ => ""
    }
}

case class Column(name: String,
                  colType: String = "",
                  nullable: Boolean = true
                 ) extends MemSQLColumn {

  override def toQueryFragment: QueryFragment =
    QueryFragment().raw(
      Seq(quotedName, colType, nullExpression, defaultExpression(None)).mkString(" "))
}

case class AdvancedColumn(name: String,
                          colType: String,
                          nullable: Boolean = true,
                          persisted: Option[String] = None,
                          defaultSQL: Option[String] = None
                         ) extends MemSQLColumn {

  override def toQueryFragment: QueryFragment = {
    val qf = QueryFragment()

    // Add the column name
    qf.raw(quotedName).space

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
}
