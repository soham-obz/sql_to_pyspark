from pyspark.sql import functions as F

# Assuming we have DataFrames 'payments' and 'subscriptions'
result = payments.alias('p').join(
    subscriptions.alias('s'),
    payments.sub_id == subscriptions.id,
    'inner'
).select(
    F.col('p.id'),
    F.col('s.status')
)