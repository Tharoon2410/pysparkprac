from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import rank,col,desc
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("Example").getOrCreate()

# Corrected data definition
data = [
    (1, 'Ram', 20000, 10),
    (2, 'Partha', 200000, 20),
    (3, 'Vishnu', 30000, 20),
    (4, 'Karthi', 50000, 20),
    (5, 'Priya', 70000, 10)
]

# Corrected schema definition
schema = StructType([
    StructField("empno", IntegerType(), True),
    StructField("ename", StringType(), True),
    StructField("Salary", IntegerType(), True),
    StructField("deptno", IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)
df.show()

# Define window specification
win = Window.partitionBy("deptno").orderBy(desc("Salary"))

# Add rank column
df_with_rank = df.withColumn("rnk", rank().over(win))
df_with_rank.show()
final = df_with_rank.filter(col("rnk") == 2).drop("rnk")
final.show()

#=======anothermethod========
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("SecondHighestSalary").getOrCreate()

# Example DataFrame (replace with your actual DataFrame)
data = [(1, "John", 50000), (2, "Jane", 60000), (3, "Jim", 70000), (4, "Jake", 80000), (5, "Jill", 75000)]
columns = ["id", "name", "salary"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Method to find second highest salary using DataFrame API
second_highest_salary_df = df.orderBy(col("salary"), ascending=False).limit(2)

# Get the second highest salary
second_highest_salary = second_highest_salary_df.collect()[1]['salary']

print(f"Second highest salary is: {second_highest_salary}")
