Here's the PySpark DataFrame code equivalent to the SQL query:

from pyspark.sql.functions import col

df = spark.table("orders")
result = df.select("id", "amount").filter(col("amount") > 100)