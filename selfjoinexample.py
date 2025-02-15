from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, count,lit

# Assuming Spark session is created
spark = SparkSession.builder.appName("SelfJoinExample").getOrCreate()
# Load the DataFrames for the tables orders and products
data =[(1, 1, 1),
(1, 1, 2),
(1, 1, 3),
(2, 2, 1),
(2, 2, 2),
(2, 2, 4),
(3, 1, 5)]
columns=("order_id","customer_id","product_id")
data1=[(1, 'A'),
(2, 'B'),
(3, 'C'),
(4, 'D'),
(5, 'E')]
columns1=("id","name")
orders_df = spark.createDataFrame(data,columns)
products_df = spark.createDataFrame(data1,columns1)
orders_df.show()
products_df.show()
# Perform the self-join on the 'orders' table with the condition (p1.product_id < p2.product_id)
a_df = orders_df.alias('p1').join(
    orders_df.alias('p2'),
    (col('p1.order_id') == col('p2.order_id')) & (col('p1.product_id') < col('p2.product_id'))
).select(
    col('p1.product_id').alias('p1'),
    col('p2.product_id').alias('p2')
)

# Join with the 'products' table for both p1 and p2
result_df = a_df.join(
    products_df.alias('b'),
    a_df['p1'] == products_df['id'],
    'inner'
).join(
    products_df.alias('b1'),
    a_df['p2'] == col("b1.id"),
    'inner'
).select(
    # Explicitly specify the column names using the aliases
    concat(col('b.name'), lit(''), col('b1.name')).alias('product_combo')  
)

# Group by the concatenated product names and count their frequencies
final_df = result_df.groupBy('product_combo').agg(
    count('product_combo').alias('purchase_freq')
)

# Show the final result
final_df.show()