from pyspark.sql import functions as F

payments = spark.table("payments").alias("p")
subscriptions = spark.table("subscriptions").alias("s")

result = payments.join(
    subscriptions,
    payments.sub_id == subscriptions.id,
    "inner"
).select(
    payments.id,
    subscriptions.status
)