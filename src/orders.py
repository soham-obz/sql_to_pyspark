Here's the PySpark DataFrame code equivalent to the given SQL query:

from pyspark.sql.functions import col

df_a = spark.table("a")
df_b = spark.table("b")

result = df_a.join(df_b, df_a.id == df_b.id).select(df_a.id, df_b.name)