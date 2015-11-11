package com.memsql.spark.interface.api

import com.memsql.spark.etl.api._

case class PipelineInstance(extractor: Extractor,
                            extractConfig: PhaseConfig,
                            transformer: Transformer,
                            transformConfig: PhaseConfig,
                            loader: Loader,
                            loadConfig: PhaseConfig)
