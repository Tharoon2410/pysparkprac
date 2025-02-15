from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder.appName("SMS Data").getOrCreate()

# Define schema for the table
data = [
    ('2020-04-01', 'Avinash', 'Vibhor', 10),
    ('2020-04-01', 'Vibhor', 'Avinash', 20),
    ('2020-04-01', 'Avinash', 'Pawan', 30),
    ('2020-04-01', 'Pawan', 'Avinash', 20),
    ('2020-04-01', 'Vibhor', 'Pawan', 5),
    ('2020-04-01', 'Pawan', 'Vibhor', 8),
    ('2020-04-01', 'Vibhor', 'Deepak', 50)
]

columns = ["sms_date", "sender", "receiver", "sms_no"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Apply logic to calculate p1, p2 using the case-when equivalent in PySpark
df_with_p1_p2 = df.withColumn(
    "p1", when(col("sender") < col("receiver"), col("sender")).otherwise(col("receiver"))
).withColumn(
    "p2", when(col("sender") > col("receiver"), col("sender")).otherwise(col("receiver"))
)
# here < compare in alphabetical order e.g, avinash and vibhor a is first in alphabet order so in < condition a comes first and > condition vibhor comes first (opp of lesser) 
# Corrected 'expr' usage for p1 and p2 columns
df_with_p2_p1 = df.withColumn(
    "p1", expr("""case when sender < receiver then sender else receiver end""") # Removed extra parentheses
).withColumn(
    "p2", expr("""case when sender > receiver then sender else receiver end""") # Removed extra parentheses
).show()
# Group by sms_date, p1, p2 and calculate the sum of sms_no
result = df_with_p1_p2.groupBy("sms_date", "p1", "p2").agg(
    sum("sms_no").alias("total_sms")
)

# Show the result
result.show()