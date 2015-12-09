import time
from pystreamliner.api import Transformer

class InitializeExceptionTransformer(Transformer):
    def initialize(self, sql_context, logger):
        raise Exception("initialize is raising an exception")
