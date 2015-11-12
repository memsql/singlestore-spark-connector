from pystreamliner.api import Extractor

class SyntaxErrorExtractor(Extractor):
    def next(self, streaming_context, time, sql_context, config, interval, logger):
        this is not valid python
