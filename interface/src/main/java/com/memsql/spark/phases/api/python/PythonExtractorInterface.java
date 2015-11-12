package com.memsql.spark.phases.api.python;

import java.io.Serializable;

import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.DataFrame;

import com.memsql.spark.etl.utils.PhaseLogger;

/**
 * Python Pipeline Extractor interface.
 * This interface is implemented by Extractors in Python via Py4J.
 */
public interface PythonExtractorInterface extends Serializable {
    /**
     * Initialization code for your Extractor.
     * This is called after instantiation of your Extractor and before [[next]].
     * The default implementation does nothing.
     *
     * @param ssc The [[org.apache.spark.streaming.StreamingContext]] that is used to run this pipeline.
     * @param sqlContext The [[org.apache.spark.sql.SQLContext]] that is used to run this pipeline.
     * @param config The Extractor configuration string passed from MemSQL Ops.
     * @param batchInterval The batch interval passed from MemSQL Ops.
     * @param logger A logger instance that is integrated with MemSQL Ops.
     */
    void Py4JInitialize(StreamingContext ssc, SQLContext sqlContext, String config, Long batchInterval, PhaseLogger logger);

    /**
     * Cleanup code for your Extractor.
     * This is called after your pipeline has terminated.
     * The default implementation does nothing.
     *
     * @param ssc The [[org.apache.spark.streaming.StreamingContext]] that is used to run this pipeline.
     * @param sqlContext The [[org.apache.spark.sql.SQLContext]] that is used to run this pipeline.
     * @param config The Extractor configuration string passed from MemSQL Ops.
     * @param batchInterval The batch interval passed from MemSQL Ops.
     * @param logger A logger instance that is integrated with MemSQL Ops.
     */
    void Py4JCleanup(StreamingContext ssc, SQLContext sqlContext, String config, Long batchInterval, PhaseLogger logger);

    /**
     * Compute the next [[org.apache.spark.sql.DataFrame]] of extracted data.
     *
     * @param ssc The [[org.apache.spark.streaming.StreamingContext]] that is used to run this pipeline.
     * @param time The timestamp from which data is being extracted.
     * @param sqlContext The [[org.apache.spark.sql.SQLContext]] that is used to create [[org.apache.spark.sql.DataFrame]]s.
     * @param config The Extractor configuration string passed from MemSQL Ops.
     * @param batchInterval The batch interval passed from MemSQL Ops.
     * @param logger A logger instance that is integrated with MemSQL Ops.
     * @return A [[org.apache.spark.sql.DataFrame]] with your extracted data. If it is not null,
     *         it will be passed through the rest of the pipeline.
     */
    DataFrame Py4JNext(StreamingContext ssc, Long time, SQLContext sqlContext, String config, Long batchInterval, PhaseLogger logger);
}
