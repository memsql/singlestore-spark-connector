from pystreamliner.api import Extractor

class CleanupExceptionExtractor(Extractor):
    def next(self, streaming_context, time, sql_context, config, interval, logger):
        rdd = streaming_context._sc.parallelize([[0]])
        return sql_context.createDataFrame(rdd, ["foo"])

    def cleanup(self, streaming_context, sql_context, config, interval, logger):
        raise Exception("cleanup is raising an exception")
