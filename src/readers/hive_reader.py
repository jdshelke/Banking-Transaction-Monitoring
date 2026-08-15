class HiveReader:
    def __init__(self, spark, database):
        self.spark = spark
        self.database = database
        
    def read_table(self, table_name):
        df = self.spark.table(f"{self.database}.{table_name}")

        return df