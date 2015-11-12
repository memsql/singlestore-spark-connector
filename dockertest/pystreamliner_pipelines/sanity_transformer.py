from pystreamliner.api import Transformer

class SanityTransformer(Transformer):
    def transform(self, sql_context, dataframe, config, logger):
        logger.error("transformer error message")

        return dataframe.select((dataframe["foo_int"] * 2).alias("foo_int_doubled"), dataframe["foo_float"], dataframe["foo_bool"], dataframe["foo_str"])
