from pystreamliner.api import Extractor

class SanityExtractor(Extractor):
    def initialize(self, spark_context, sql_context, interval, logger):
        self.i = 1

    def next(self, spark_context, sql_context, interval, logger):
        logger.info("extractor info message")

        rdd = spark_context.parallelize([[self.i, float(self.i), self.i % 2 == 0, str(self.i)]])
        self.i += 1
        return sql_context.createDataFrame(rdd, ["foo_int", "foo_float", "foo_bool", "foo_str"])
