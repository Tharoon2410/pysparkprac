from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder \
        .master("local") \
        .appName("Hands-on PySpark on Google Colab") \
        .getOrCreate()

data = [
    ("John Doe", 101, 5000), ("Karthi", 101, 5000), ("Jane Smith", 102, 6000), ("Michael Brown", 101, 7000),
    ("Emily Davis", 103, 8000),("James Wilson", 102, 9000), ("Abi", 102, 9500), ("Prathulya", 102, 9000),
    ("Kumar", 104, 4000), ("Selva", 104, 4000), ("Partha", 104, 7500), ("Krishna", 104, 7500)]
schema = StructType([
    StructField("Emp_name", StringType(), True),
    StructField("Depart_ID", IntegerType(), True),
    StructField("Salary", IntegerType(), True)
])
df = spark.createDataFrame(data, schema)
df.show()
# Define a Window specification
window_spec = Window.partitionBy("Depart_ID").orderBy("Salary")

# Add rank and dense_rank columns
df_with_ranks = df.withColumn("Rank", rank().over(window_spec)) \
                  .withColumn("Dense_Rank", dense_rank().over(window_spec))

# Show the DataFrame with ranks
df_with_ranks.show()