##leetcode 1270##
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder.appName("Employees").getOrCreate()

# Define schema
schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("employee_name", StringType(), True),
    StructField("manager_id", IntegerType(), True)
])

# Create data
data = [
    (1, "Boss", 1),
    (3, "Alice", 3),
    (2, "Bob", 1),
    (4, "Daniel", 2),
    (7, "Luis", 4),
    (8, "Jhon", 3),
    (9, "Angela", 8),
    (77, "Robert", 1)
]

# Create DataFrame
employees_df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
employees_df.show()

#join_df = employees_df.alias("e1") \
#    .join(employees_df.alias("e2"), col("e1.manager_id") == col("e2.employee_id"), "left") \
#    .join(employees_df.alias("e3"), col("e3.manager_id") == col("e2.employee_id"), "left") \
#    .filter(
#    (col("e3.manager_id") == 1) &
#    (col("e1.employee_id") != col("e1.manager_id"))
#) \
#    .select("e1.employee_id").distinct()
#join_df.show()
# The head employee_id
head_id = 1

# Find first level of employees who directly report to head
level1_df = employees_df.filter(col("manager_id") == head_id).select("employee_id")

# Find second level of employees who report to first level
level2_df = employees_df.alias("e1").join(
    level1_df.alias("l1"),
    col("e1.manager_id") == col("l1.employee_id"),
    "inner"
).select("e1.employee_id")

# Find third level of employees who report to second level
level3_df = employees_df.alias("e2").join(
    level2_df.alias("l2"),
    col("e2.manager_id") == col("l2.employee_id"),
    "inner"
).select("e2.employee_id")

# Combine all levels
final_df = level1_df.union(level2_df).union(level3_df).dropDuplicates()

final_df = final_df.filter(col("employee_id") != head_id)
# Show the result
final_df.show()
employees_df.createOrReplaceTempView("employees")
spark.sql("select e1.employee_id from employees e1 LEFT JOIN employees e2 on e1.manager_id = e2.employee_id LEFT JOIN employees e3 on e3.manager_id = e2.employee_id where e3.manager_id =1 AND e1.employee_id <> e1.manager_id").show()