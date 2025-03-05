#business city table has data from the day udaan has started operation
#write a SQL to identify yearwise count of new cities where udaan started their operations
#############################
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a Spark session
spark = SparkSession.builder.master("local").appName("BusinessCity").getOrCreate()

# Create the data (same as the insert data)
data = [
    ('2020-01-02', 3),
    ('2020-07-01', 7),
    ('2021-01-01', 3),
    ('2021-02-03', 19),
    ('2022-12-01', 3),
    ('2022-12-15', 3),
    ('2022-02-28', 12)
]

# Define the schema
columns = ['business_date', 'city_id']

# Create the DataFrame
df = spark.createDataFrame(data, columns)

# Step 1: Get the minimum year for each city_id
df_min_year = df.withColumn('year', year(col('business_date'))) \
    .groupBy('city_id') \
    .agg(min('year').alias('year'))
print("df_min_year")
df_min_year.show()    

# Step 2: Count the number of city_id for each year
op_df = df_min_year.groupBy('year').agg(count('city_id').alias('count')) \
    .orderBy('year')

# Show the result
op_df.show()
