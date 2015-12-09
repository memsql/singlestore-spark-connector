from pystreamliner.api import Transformer

class TransformExceptionTransformer(Transformer):
    def transform(self, sql_context, dataframe, logger):
        raise Exception("transform is raising an exception")
