from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import last, col

# Initialize SparkSession
spark = SparkSession.builder.appName("FillNulls").getOrCreate()

# Sample data
data = [
    ('chocolates', '5-star'),
    (None, 'dairy milk'),
    (None, 'perk'),
    (None, 'eclair'),
    ('Biscuits', 'britannia'),
    (None, 'good day'),
    (None, 'boost')
]

# Schema
schema = ["category", "brand_name"]

# Create DataFrame
brands_df = spark.createDataFrame(data, schema=schema)

# Define the window specification
window_spec = Window.orderBy(expr("NULL")).rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Fill null values in 'category' column using last non-null value
result_df = brands_df.withColumn(
    "category",
    last("category", True).over(window_spec)
)

# Show the result
result_df.show()

brands_df.createOrReplaceTempView("brands")
spark.sql("with cte as(\
Select *\
,count(category)over(order by (Select null) rows between unbounded preceding and 0 following ) as rn \
from brands)\
Select first_value(category)over(partition by rn order by rn ) as category, brand_name from cte").show()