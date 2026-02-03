from pyspark.sql.functions import col

orders = spark.table("orders")

result = orders.filter(col("status") == "DELIVERED")