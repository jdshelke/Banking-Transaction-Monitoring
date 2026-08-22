def createGoldTables(spark, table_name):

    # Create Gold database
    spark.sql("""
        CREATE DATABASE IF NOT EXISTS gold
    """)

    if table_name == "customer_transaction_summary":

        spark.sql("""
            CREATE TABLE IF NOT EXISTS gold.customer_transaction_summary (
                customer_id BIGINT,
                customer_name STRING,

                total_transactions BIGINT,
                total_transaction_amount DECIMAL(28,2),
                average_transaction_amount DECIMAL(22,6),
                min_transaction_amount DECIMAL(18,2),
                max_transaction_amount DECIMAL(18,2),

                credit_transaction_count BIGINT,
                debit_transaction_count BIGINT,

                credit_amount DECIMAL(28,2),
                debit_amount DECIMAL(28,2),

                first_transaction_date DATE,
                last_transaction_date DATE,

                active_account_count BIGINT,
                gold_ingestion_time TIMESTAMP
            )
            STORED AS PARQUET
        """)