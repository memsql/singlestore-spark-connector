from pystreamliner.api import Extractor

class DefaultExtractor(Extractor):
    def next(self, spark_context, sql_context, interval, logger):
        rdd = spark_context.parallelize([[0]])
        return sql_context.createDataFrame(rdd, ["foo"])
