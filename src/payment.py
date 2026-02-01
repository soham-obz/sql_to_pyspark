from pyspark.sql.functions import col

result = spark.table("payments").alias("p") \
    .join(spark.table("subscriptions").alias("s"), 
          col("p.sub_id") == col("s.id"), "inner") \
    .select(col("p.id"), col("s.status"))