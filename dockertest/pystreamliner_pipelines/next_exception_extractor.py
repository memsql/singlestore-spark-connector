from pystreamliner.api import Extractor

class NextExceptionExtractor(Extractor):
    def next(self, streaming_context, time, sql_context, config, interval, logger):
        raise Exception("next is raising an exception")
