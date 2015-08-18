package com.memsql.spark.interface.api

import com.memsql.spark.etl.api._
import com.memsql.spark.etl.api.configs.PhaseConfig

case class PipelineInstance[Any](extractor: Extractor[Any],
                                 extractConfig: PhaseConfig,
                                 transformer: Transformer[Any],
                                 transformConfig: PhaseConfig,
                                 loader: Loader,
                                 loadConfig: PhaseConfig)
