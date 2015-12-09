from pystreamliner.api import Transformer

class SyntaxErrorTransformer(Transformer):
    def transform(self, sql_context, dataframe, logger):
        this is not valid python
