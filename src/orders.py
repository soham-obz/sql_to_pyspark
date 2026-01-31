from pyspark.sql.functions import col

result = a.join(b, a.id == b.id).select(a.id, b.name)