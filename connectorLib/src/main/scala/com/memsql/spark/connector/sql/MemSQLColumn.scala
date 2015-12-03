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
                  colType: String,
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
    qf.raw(quotedName)

    // Optionally: AS expression PERSISTED
    persisted.map(p => qf.raw(" AS ").raw(p).raw(" PERSISTED "))

    // column type
    qf.raw(colType).space

    // null/not null modifier
    qf.raw(nullExpression).space

    // If not computed, maybe add default SQL expression
    if (persisted.isEmpty) {
      qf.raw(defaultExpression(defaultSQL))
    }

    qf
  }
}
