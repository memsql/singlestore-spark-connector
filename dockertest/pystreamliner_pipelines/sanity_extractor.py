from pystreamliner.api import Extractor

class SanityExtractor(Extractor):
    def initialize(self, streaming_context, sql_context, config, interval, logger):
        self.i = 1

    def next(self, streaming_context, time, sql_context, config, interval, logger):
        logger.info("extractor info message")

        rdd = streaming_context._sc.parallelize([[self.i, float(self.i), self.i % 2 == 0, str(self.i)]])
        self.i += 1
        return sql_context.createDataFrame(rdd, ["foo_int", "foo_float", "foo_bool", "foo_str"])
