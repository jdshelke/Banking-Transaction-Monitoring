from pyspark.sql.functions import col, count, sum, avg, min, max, when


def create_customer_transaction_summary(customers_df, accounts_df, transactions_df):

    joined_df = customers_df.alias("c") \
                            .join( accounts_df.alias("a"), 
                                    col("c.customer_id") == col("a.customer_id"),
                                    "left" 
                                ) \
                            .join(transactions_df.alias("t"),
                                  col("t.account_id") == col("a.account_id"),
                                  "left"
                                  )\
                            .select(
                                col("c.customer_id"),
                                col("c.name").alias("customer_name"),
                                col("a.account_id"),
                                col("a.status").alias("account_status"),
                                col("t.transaction_id"),
                                col("t.txn_date"),
                                col("t.txn_type"),
                                col("t.amount")
                            )

    customer_transaction_summary_df = joined_df \
                            .groupBy(col("customer_id"), col("customer_name")) \
                            .agg(
                                count(col("transaction_id")).alias("total_transactions"),
                                sum(col("amount")).alias("total_transaction_amount"),
                                avg(col("amount")).alias("average_transaction_amount"),
                                min(col("amount")).alias("min_transaction_amount"),
                                max(col("amount")).alias("max_transaction_amount"),
                                sum(
                                    when(col("txn_type") == "Deposit", 1)\
                                    .when(col("txn_type") == "Interest Credit", 1)\
                                    .when(col("txn_type") == "Transfer In", 1)\
                                    .otherwise(0)
                                    ).alias("credit_transaction_count"),
                                sum(
                                    when(col("txn_type") == "Withdrawal", 1)\
                                    .when(col("txn_type") == "Fee Debit", 1)\
                                    .when(col("txn_type") == "Transfer Out", 1)\
                                    .otherwise(0)
                                    ).alias("debit_transaction_count"),
                                sum(
                                    when(col("txn_type") == "Deposit", col("amount"))\
                                    .when(col("txn_type") == "Interest Credit", col("amount"))\
                                    .when(col("txn_type") == "Transfer In", col("amount"))\
                                    .otherwise(0)
                                    ).alias("credit_amount"),
                                sum(
                                    when(col("txn_type") == "Withdrawal", col("amount"))\
                                    .when(col("txn_type") == "Fee Debit", col("amount"))\
                                    .when(col("txn_type") == "Transfer Out", col("amount"))\
                                    .otherwise(0)
                                    ).alias("debit_amount"),
                                min(col("txn_date")).alias("first_transaction_date"),
                                max(col("txn_date")).alias("last_transaction_date"),
                            )

    active_accounts_df = accounts_df \
                            .filter(
                                    col("status") == "Active"
                                    ) \
                            .groupBy(col("customer_id")) \
                            .agg(
                                count("account_id").alias("active_account_count")
                                )

    final_df = customer_transaction_summary_df.alias("cts") \
                            .join(
                                active_accounts_df.alias("aa"),
                                col("cts.customer_id") == col("aa.customer_id"),
                                "left"
                            ) \
                            .select(
                                col("cts.customer_id"),
                                col("cts.customer_name"),
                                col("cts.total_transactions"),
                                col("cts.total_transaction_amount"),
                                col("cts.average_transaction_amount"),
                                col("cts.min_transaction_amount"),
                                col("cts.max_transaction_amount"),
                                col("cts.credit_transaction_count"),
                                col("cts.debit_transaction_count"),
                                col("cts.credit_amount"),
                                col("cts.debit_amount"),
                                col("cts.first_transaction_date"),
                                col("cts.last_transaction_date"),
                                col("aa.active_account_count")
                            )

    return final_df
