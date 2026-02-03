from pyspark.sql.functions import col

orders = spark.table("orders")

result = orders.select("order_id").filter(col("status") == "DELIVERED")