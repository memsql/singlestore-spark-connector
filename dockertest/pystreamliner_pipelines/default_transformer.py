from pystreamliner.api import Transformer

class DefaultTransformer(Transformer):
    def transform(self, sql_context, dataframe, logger):
        return dataframe
