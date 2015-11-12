import time
from pystreamliner.api import Extractor

class InitializeExceptionExtractor(Extractor):
    def initialize(self, streaming_context, sql_context, config, interval, logger):
        raise Exception("initialize is raising an exception")
