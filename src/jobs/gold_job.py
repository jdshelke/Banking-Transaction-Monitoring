from pyspark.sql import SparkSession

import argparse
from src.readers.hive_reader import HiveReader
from src.writers.gold_writer import GoldWriter

from src.transformations.gold import (
    customer_transaction_summary
)


class GoldJob:
    def __init__(self, spark):
        self.spark = spark

        self.hive_reader = HiveReader(self.spark, "silver")
        self.gold_writer = GoldWriter(self.spark)

    def process_table(self, table_name, transform_function, required_tables):

        table_dfs = {}

        for table in required_tables:
            table_dfs[f"{table}_df"] = self.hive_reader.read_table(table)

        gold_df = transform_function(**table_dfs)

        self.gold_writer.write_hive_table(gold_df, table_name)


if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("BankingTransactionMonitoring-GoldJob") \
        .enableHiveSupport() \
        .getOrCreate()


    gold_job = GoldJob(spark)

    transformations = {
        "customer_transaction_summary": customer_transaction_summary.create_customer_transaction_summary
    }

    required_tables_map = {
        "customer_transaction_summary": ["customers", "accounts", "transactions"]
    }

    parser = argparse.ArgumentParser()

    parser.add_argument("--table", required=True, help="Parquet files to Hive tables")

    args = parser.parse_args()

    transform_function = transformations[args.table]
    required_tables = required_tables_map[args.table]

    gold_job.process_table(args.table, transform_function, required_tables)

    spark.stop()