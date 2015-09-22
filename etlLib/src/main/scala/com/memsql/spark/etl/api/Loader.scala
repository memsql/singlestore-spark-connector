package com.memsql.spark.etl.api

import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.spark.sql.DataFrame

/**
 * Pipeline Loader interface.
 */
abstract class Loader extends Serializable {
  /**
   * Loads a [[org.apache.spark.sql.DataFrame]].
   *
   * @param dataframe The data and schema to be loaded.
   * @param loadConfig The Loader configuration passed from MemSQL Ops.
   * @param logger A logger instance that is integrated with MemSQL Ops.
   * @return The total number of rows successfully loaded.
   */
  def load(dataframe: DataFrame, loadConfig: PhaseConfig, logger: PhaseLogger): Long
}
