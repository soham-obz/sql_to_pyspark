To convert this SQL query into idiomatic PySpark DataFrame code, I'll use the DataFrame API. Here's the equivalent PySpark code:

from pyspark.sql.functions import col

payments_df = spark.table("payments")
subscriptions_df = spark.table("subscriptions")

result_df = payments_df.alias("p").join(
    subscriptions_df.alias("s"),
    col("p.sub_id") == col("s.id"),
    "inner"
).select(
    col("p.id"),
    col("s.status")
)

result_df.show()