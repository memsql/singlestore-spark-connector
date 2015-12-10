from pystreamliner.api import Extractor

class MultipleTablesExtractor(Extractor):
    def initialize(self, spark_context, sql_context, interval, logger):
        self.i = 0

    def next(self, spark_context, sql_context, interval, logger):
        if self.i % 2 == 0:
            df = sql_context.read.format("com.memsql.spark.connector").load("reference.data")    
        else:
            sql_context.set_database("reference")
            df = sql_context.table("data")

        self.i += 1

        return df
