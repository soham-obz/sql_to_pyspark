Here's the PySpark DataFrame code equivalent to the given SQL query:

from pyspark.sql.functions import col

result = spark.table("orders").select("id", "amount").filter(col("amount") > 100)