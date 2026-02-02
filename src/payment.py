from pyspark.sql.functions import col

payments = spark.table("payments")
subscriptions = spark.table("subscriptions")

result = (
    payments.alias("p")
    .join(subscriptions.alias("s"), col("p.sub_id") == col("s.id"))
    .select(col("p.id"), col("s.status"))
)