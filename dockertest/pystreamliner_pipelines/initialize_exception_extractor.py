import time
from pystreamliner.api import Extractor

class InitializeExceptionExtractor(Extractor):
    def initialize(self, spark_context, sql_context, interval, logger):
        raise Exception("initialize is raising an exception")
