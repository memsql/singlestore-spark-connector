package com.memsql.spark.connector.util

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.types.{MetadataBuilder, Metadata}

object MetadataUtils {
  val METADATA_ORIGINAL_NAME = "originalColumnName"

  def preserveOriginalName(expr: NamedExpression): Metadata = {
    val meta = expr.metadata
    if (!meta.contains(METADATA_ORIGINAL_NAME)) {
      new MetadataBuilder()
        .withMetadata(meta)
        .putString(METADATA_ORIGINAL_NAME, expr.name)
        .build
    } else {
      meta
    }
  }

  def getOriginalName(expr: NamedExpression): Option[String] = {
    if (expr.metadata.contains(METADATA_ORIGINAL_NAME)) {
      Some(expr.metadata.getString(METADATA_ORIGINAL_NAME))
    } else {
      None
    }
  }
}
