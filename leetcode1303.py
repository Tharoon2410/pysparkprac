##select employee_id,count(employee_id) over(partition by team_id order by team_id) AS team_size from Employee

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize SparkSession
spark = SparkSession.builder.appName("EmployeeTeam").getOrCreate()

# Define schema
schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("team_id", IntegerType(), True)
])

# Create data
data = [
    (1, 8),
    (2, 8),
    (3, 8),
    (4, 7),
    (5, 9),
    (6, 9)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()

count_df = df.withColumn("team_size",count("employee_id").over(Window.partitionBy("team_id").orderBy("team_id"))).select("employee_id","team_size")
count_df.show()
