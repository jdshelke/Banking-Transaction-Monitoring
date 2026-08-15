from pyspark.sql import SparkSession

from src.utils import config_reader
from src.readers.mysql_reader import MySQLReader
from src.writers.bronze_writer import BronzeWriter

class BronzeIngestion:
    def __init__(self, spark, mysql_config, hdfs_config):
        self.spark = spark
        self.mysql_config = mysql_config
        self.hdfs_config = hdfs_config

        self.mysql_reader = MySQLReader(self.spark, 
                                           self.mysql_config["host"], 
                                           self.mysql_config["port"], 
                                           self.mysql_config["database"], 
                                           self.mysql_config["username"], 
                                           self.mysql_config["password"], 
                                           self.mysql_config["driver"])

        self.bronze_writer = BronzeWriter(self.hdfs_config["namenode"], self.hdfs_config["base_path"])

    def ingestion(self, table_name):
        df = self.mysql_reader.read_table(table_name)

        self.bronze_writer.write_parquet(df, table_name)

if __name__ == "__main__":

    config = config_reader.load_config(
        "config/config.yaml"
    )

    spark = SparkSession.builder \
        .appName("BankingTransactionMonitoring-BronzeIngestion") \
        .master("local[*]") \
        .getOrCreate()

    mysql_config = config["mysql"]
    hdfs_config = config["hdfs"]

    bronze_ingestion = BronzeIngestion(
        spark,
        mysql_config,
        hdfs_config
    )

    tables = [
        "loans",
        "support_tickets",
        "transactions",
        "customers",
        "employees",
        "loan_payments",
        "cards",
        "card_transactions",
        "accounts",
        "branches"
    ]

    for table in tables:
        bronze_ingestion.ingestion(table)

    spark.stop()