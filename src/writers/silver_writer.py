from pyspark.sql.functions import current_timestamp
from src.sql.create_silver_tables import createSilverTables


class SilverWriter:

    def __init__(self, spark):
        self.spark = spark

    def write_hive_table(self, df, table_name, primary_key):

        full_table_name = f"silver.{table_name}"

        # Add Silver audit column
        df = df.withColumn(
            "silver_ingestion_time",
            current_timestamp()
        )

        if self.spark.catalog.tableExists(full_table_name):

            # Read existing Silver table
            existing_df = self.spark.table(full_table_name)

            # Keep existing records whose primary key
            # does not exist in the incoming dataframe
            old_records_df = existing_df.join(
                df.select(primary_key).distinct(),
                on=primary_key,
                how="left_anti"
            )

            # Combine old records with incoming records
            silver_df = old_records_df.unionByName(df)

            # Overwrite Silver table
            silver_df.write \
                .mode("overwrite") \
                .format("parquet") \
                .saveAsTable(full_table_name)

            print(f"Upsert completed: {full_table_name}")

        else:

            # Create Silver table
            createSilverTables(self.spark, table_name)

            # Get target table column order
            target_columns = self.spark.table(
                full_table_name
            ).columns

            # Write data into Silver table
            df.select(target_columns) \
                .write \
                .mode("append") \
                .insertInto(full_table_name)

            print(f"Silver table created and data written: {full_table_name}")