from pystreamliner import CreateMode, SaveMode, CompressionType
from pystreamliner.api import Transformer

class MultipleTablesTransformer(Transformer):
    def initialize(self, sql_context, logger):
        self.i = 0

    def transform(self, sql_context, dataframe, logger):
        # test various methods of saving the dataframe
        if self.i % 4 == 0:
            dataframe.write.format("com.memsql.spark.connector").save("pystreamliner.table%d" % self.i)
        elif self.i % 4 == 1:
            dataframe.saveToMemSQL("table%d" % self.i, "pystreamliner", save_mode=SaveMode.Append,
                                   create_mode=CreateMode.DatabaseAndTable, on_duplicate_key_sql="foo = 0")
        elif self.i % 4 == 2:
            dataframe.saveToMemSQL("table%d" % self.i, "pystreamliner", insert_batch_size=1)
        else:
            dataframe.saveToMemSQL("table%d" % self.i, "pystreamliner", load_data_compression=CompressionType.GZip,
                                   use_keyless_sharding_optimization=True)

        self.i += 1

        return dataframe
