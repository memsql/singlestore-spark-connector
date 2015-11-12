from pystreamliner.api import Extractor

class DefaultExtractor(Extractor):
    def next(self, streaming_context, time, sql_context, config, interval, logger):
        rdd = streaming_context._sc.parallelize([[0]])
        return sql_context.createDataFrame(rdd, ["foo"])
