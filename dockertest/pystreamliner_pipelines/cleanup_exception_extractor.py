from pystreamliner.api import Extractor

class CleanupExceptionExtractor(Extractor):
    def next(self, sc, sql_context, interval, logger):
        rdd = sc.parallelize([[0]])
        return sql_context.createDataFrame(rdd, ["foo"])

    def cleanup(self, sc, sql_context, interval, logger):
        raise Exception("cleanup is raising an exception")
