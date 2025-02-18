from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('selfjoinunderstanding').getOrCreate()

# Create DataFrame for int_orders
data = [
    (30, '1995-07-14', 9, 1, 460),
    (10, '1996-08-02', 4, 2, 540),
    (40, '1998-01-29', 7, 2, 2400),
    (50, '1998-02-03', 6, 7, 600),
    (60, '1998-03-02', 6, 7, 720),
    (70, '1998-05-06', 9, 7, 150),
    (20, '1999-01-30', 4, 8, 1800)
]

# Define schema for the data
schema = ["order_number", "order_date", "cust_id", "salesperson_id", "amount"]

df = spark.createDataFrame(data,schema)
# Perform the SQL equivalent operation
# Step 1: Join the DataFrame with itself on salesperson_id
df_joined = df.alias('a').join(df.alias('b'),on=col('a.salesperson_id') == col('b.salesperson_id'),how="left")

# Step 2: Group by the necessary columns and apply the condition in 'having'
result_df = df_joined.groupBy("a.order_number","a.order_date","a.cust_id","a.salesperson_id","a.amount")\
.agg(max("b.amount").alias('max_b_amount')).filter(col('a.amount') >= col('max_b_amount'))\
.select("a.order_number","a.order_date","a.cust_id","a.salesperson_id","a.amount")
result_df.show()