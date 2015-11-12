from pystreamliner.api import Transformer

class SyntaxErrorTransformer(Transformer):
    def transform(self, sql_context, dataframe, config, logger):
        this is not valid python
