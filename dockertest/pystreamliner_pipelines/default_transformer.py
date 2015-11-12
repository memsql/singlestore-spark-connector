from pystreamliner.api import Transformer

class DefaultTransformer(Transformer):
    def transform(self, sql_context, dataframe, config, logger):
        return dataframe
