from pystreamliner.api import Transformer

class CleanupExceptionTransformer(Transformer):
    def transform(self, sql_context, dataframe, config, logger):
        return dataframe

    def cleanup(self, sql_context, config, logger):
        raise Exception("cleanup is raising an exception")

