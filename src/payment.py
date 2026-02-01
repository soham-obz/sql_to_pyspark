from pyspark.sql.functions import col

payments = spark.table("payments").alias("p")
subscriptions = spark.table("subscriptions").alias("s")

result = (
    payments
    .join(subscriptions, col("p.sub_id") == col("s.id"))
    .select(col("p.id"), col("s.status"))
)