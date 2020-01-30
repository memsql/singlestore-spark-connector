package com.memsql.spark

/*
object MetadataUtils {
  val METADATA_MEMSQL_BASE_NAME = "memsqlBaseColumnName"

  def preserveOriginalName(expr: NamedExpression): Metadata = {
    val meta = expr.metadata
    if (!meta.contains(METADATA_ORIGINAL_NAME))
      new MetadataBuilder()
        .withMetadata(meta)
        .putString(METADATA_ORIGINAL_NAME, expr.name)
        .build
    else meta
  }

  def getOriginalName(expr: NamedExpression): Option[String] =
    if (expr.metadata.contains(METADATA_ORIGINAL_NAME))
      Some(expr.metadata.getString(METADATA_ORIGINAL_NAME))
    else None
}
 */
