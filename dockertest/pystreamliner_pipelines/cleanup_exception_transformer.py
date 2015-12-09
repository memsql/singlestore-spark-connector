from pystreamliner.api import Transformer

class CleanupExceptionTransformer(Transformer):
    def transform(self, sql_context, dataframe, logger):
        return dataframe

    def cleanup(self, sql_context, logger):
        raise Exception("cleanup is raising an exception")

