##leetcode1596##
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.window import Window

# Create Spark session
spark = SparkSession.builder.appName("Tables").getOrCreate()

# --- Customers DataFrame ---
customers_data = [
    (1, "Alice"),
    (2, "Bob"),
    (3, "Tom"),
    (4, "Jerry"),
    (5, "John")
]

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True)
])

df1 = spark.createDataFrame(customers_data, schema=customers_schema)

# --- Orders DataFrame ---
orders_data = [
    (1, "2020-07-31", 1, 1),
    (2, "2020-07-30", 2, 2),
    (3, "2020-08-29", 3, 3),
    (4, "2020-07-29", 4, 1),
    (5, "2020-06-10", 1, 2),
    (6, "2020-08-01", 2, 1),
    (7, "2020-08-01", 3, 3),
    (8, "2020-08-03", 1, 2),
    (9, "2020-08-07", 2, 3),
    (10, "2020-07-15", 1, 2)
]

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_date", StringType(), True),  # We'll cast to DateType if needed
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True)
])

df2 = spark.createDataFrame(orders_data, schema=orders_schema)
df2 = df2.withColumn("order_date", to_date("order_date", "yyyy-MM-dd"))

# --- Products DataFrame ---
products_data = [
    (1, "keyboard", 120),
    (2, "mouse", 80),
    (3, "screen", 600),
    (4, "hard disk", 450)
]

products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("price", IntegerType(), True)
])

df3 = spark.createDataFrame(products_data, schema=products_schema)

order_df = df2.groupBy("customer_id","product_id").agg(expr("count(*) as num_ordered")).select("customer_id","product_id","num_ordered")
window_spec = Window.partitionBy("customer_id").orderBy(col("num_ordered").desc())
order_df1 = order_df.withColumn("most_frequent",first_value("num_ordered").over(window_spec))
filter_df = order_df1.filter(col("num_ordered") == col("most_frequent")).select("*")

join_df = filter_df.join(df3,"product_id","left").orderBy("customer_id","product_id").select("customer_id","product_id","product_name")
join_df.show()

df1.createOrReplaceTempView("customers")
df2.createOrReplaceTempView("orders")
df3.createOrReplaceTempView("products")
spark.sql(""" WITH cte as (select customer_id,product_id,count(*) AS num_ordered from orders group by customer_id,product_id),
cte2 AS (select *,FIRST_VALUE(NUM_ORDERED)OVER(PARTITION BY customer_id ORDER BY num_ordered DESC) AS most_frequent from cte)
select c.customer_id,c.product_id,p.product_name from cte2 c LEFT JOIN Products p on c.product_id = p.product_id WHERE c.num_ordered = c.most_frequent""")

=========================
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.window import Window

# Create Spark session
spark = SparkSession.builder.appName("Tables").getOrCreate()

# --- Customers DataFrame ---
customers_data = [
    (1, "Alice"),
    (2, "Bob"),
    (3, "Tom"),
    (4, "Jerry"),
    (5, "John")
]

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True)
])

df1 = spark.createDataFrame(customers_data, schema=customers_schema)

# --- Orders DataFrame ---
orders_data = [
    (1, "2020-07-31", 1, 1),
    (2, "2020-07-30", 2, 2),
    (3, "2020-08-29", 3, 3),
    (4, "2020-07-29", 4, 1),
    (5, "2020-06-10", 1, 2),
    (6, "2020-08-01", 2, 1),
    (7, "2020-08-01", 3, 3),
    (8, "2020-08-03", 1, 2),
    (9, "2020-08-07", 2, 3),
    (10, "2020-07-15", 1, 2)
]

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_date", StringType(), True),  # We'll cast to DateType if needed
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True)
])

df2 = spark.createDataFrame(orders_data, schema=orders_schema)
df2 = df2.withColumn("order_date", to_date("order_date", "yyyy-MM-dd"))

# --- Products DataFrame ---
products_data = [
    (1, "keyboard", 120),
    (2, "mouse", 80),
    (3, "screen", 600),
    (4, "hard disk", 450)
]

products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("price", IntegerType(), True)
])

df3 = spark.createDataFrame(products_data, schema=products_schema)
# Join Customers, Orders, and Products
tmp = df1.alias("a") \
    .join(df2.alias("b"), col("a.customer_id") == col("b.customer_id")) \
    .join(df3.alias("c"), col("b.product_id") == col("c.product_id")) \
    .select(
        col("a.customer_id"),
        col("b.product_id"),
        col("c.product_name"),
        col("b.order_id")
    )

# Calculate frequency of each product ordered by customer
tmp_freq = tmp.groupBy("customer_id", "product_id", "product_name") \
    .agg(count("order_id").alias("freq"))

# Apply DENSE_RANK partitioned by customer_id and ordered by freq DESC
window_spec = Window.partitionBy("customer_id").orderBy(col("freq").desc())
tmp_ranked = tmp_freq.withColumn("rnk", dense_rank().over(window_spec))

# Filter only the most frequently ordered products (rank = 1)
result_df = tmp_ranked.filter(col("rnk") == 1).select("customer_id", "product_id", "product_name").distinct()

# Show the final result
result_df.show()
