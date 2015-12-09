from pystreamliner.api import Extractor

class SyntaxErrorExtractor(Extractor):
    def next(self, spark_context, sql_context, interval, logger):
        this is not valid python
