from pystreamliner.api import Extractor

class NextExceptionExtractor(Extractor):
    def next(self, spark_context, sql_context, interval, logger):
        raise Exception("next is raising an exception")
