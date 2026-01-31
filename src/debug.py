Here's the PySpark DataFrame code equivalent to the provided SQL query:

from pyspark.sql.functions import col

payments = spark.table("payments")
subscriptions = spark.table("subscriptions")

result = payments.alias("p").join(
    subscriptions.alias("s"),
    col("p.sub_id") == col("s.id"),
    "inner"
).select(col("p.id"), col("s.status"))