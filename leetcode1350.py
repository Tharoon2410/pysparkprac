from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *

# Create Spark session
spark = SparkSession.builder.appName("leetcode1350").getOrCreate()

# Sample data for Departments table
departments_data = [
    (1, "Electrical Engineering"),
    (7, "Computer Engineering"),
    (13, "Business Administration")
]

# Sample data for Students table
students_data = [
    (23, "Alice", 1),
    (1, "Bob", 7),
    (5, "Jennifer", 13),
    (2, "John", 14),
    (4, "Jasmine", 77),
    (3, "Steve", 74),
    (6, "Luis", 1),
    (8, "Jonathan", 7),
    (7, "Daiana", 33),
    (11, "Madelynn", 1)
]

# Define column names
departments_columns = ["id", "name"]
students_columns = ["id", "name", "department_id"]

# Create DataFrames
departments_df = spark.createDataFrame(departments_data, departments_columns)
students_df = spark.createDataFrame(students_data, students_columns)

# Show results
departments_df.show()
students_df.show()

result_df = students_df.join(
    departments_df, students_df["department_id"] == departments_df["id"], "left_anti"
).orderBy("department_id").select("id","name")

result_df.show()