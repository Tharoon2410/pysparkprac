from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, count
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("StadiumQuery").getOrCreate()

# Create the DataFrame
data = [
    (1, '2017-07-01', 10),
    (2, '2017-07-02', 109),
    (3, '2017-07-03', 150),
    (4, '2017-07-04', 99),
    (5, '2017-07-05', 145),
    (6, '2017-07-06', 1455),
    (7, '2017-07-07', 199),
    (8, '2017-07-08', 188)
]

# Define the schema
columns = ["id", "visit_date", "no_of_people"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Create the window specification for row_number()
window_spec = Window.orderBy("visit_date")

# Add row_number and grp columns
df_with_rn_and_grp = df.filter(col("no_of_people") >= 100).withColumn("rn", row_number().over(window_spec)) \
    .withColumn("grp", col("id") - row_number().over(window_spec)) \
    

# Filter groups with at least 3 rows
result = df_with_rn_and_grp.groupBy("grp") \
    .agg(count("id").alias("grp_count")) \
    .filter(col("grp_count") >= 3) \
    .join(df_with_rn_and_grp, on="grp", how="inner") \
    .select("id"  , "visit_date", "no_of_people") \
    .orderBy("id")

# Show the result
result.show()
