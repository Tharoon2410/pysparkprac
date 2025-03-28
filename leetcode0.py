# Sample data
data = [
    (1,'Joe'),
    (2, 'Henry'),
    (3, 'sam'),
    (4, 'max')
]

# Schema
schema = ["id", "name"]

data1 = [
    (1,3),
    (2, 1)    
]
# Schema
schema = ["id", "customerid"]

spark.sql("select c.name AS Customers from customers c LEFT JOIN orders o on c.id = o.customerid where o.id IS NULL")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create Spark session
spark = SparkSession.builder \
    .appName("LeftJoinExample") \
    .getOrCreate()

# Sample data and schema for 'customers'
customer_data = [
    (1, 'Joe'),
    (2, 'Henry'),
    (3, 'Sam'),
    (4, 'Max')
]

customer_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

customers_df = spark.createDataFrame(data=customer_data, schema=customer_schema)

# Sample data and schema for 'orders'
order_data = [
    (1, 3),
    (2, 1)
]

order_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("customerid", IntegerType(), True)
])

orders_df = spark.createDataFrame(data=order_data, schema=order_schema)

# Perform LEFT JOIN using PySpark syntax


result_df = customers_df.join(
    orders_df,
    customers_df.id == orders_df.customerid,
    "left"
).filter(orders_df.id.isNull()) \
 .select(customers_df.name.alias("Customers"))

# Show the result
result_df.show()
customers_df.createOrReplaceTempView("customers")
orders_df.createOrReplaceTempView("orders")
spark.sql("select c.name as customers from customers c where c.name NOT IN(select c.name from customers c inner join orders o on c.id = o.customerid)").show()
#############################
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("EmployeesEarningMoreThanManagers") \
    .getOrCreate()

# Employee data
data = [
    (1, 'Joe', 70000, 3),
    (2, 'Henry', 80000, 4),
    (3, 'Sam', 60000, None),
    (4, 'Max', 90000, None)
]

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("managerId", IntegerType(), True)
])

# Create DataFrame
employee_df = spark.createDataFrame(data, schema=schema)

# Self join to compare employee salary with manager salary
result_df = employee_df.alias("e1") \
    .join(employee_df.alias("e2"), 
          col("e1.managerId") == col("e2.id"), 
          "inner") \
    .filter(col("e1.salary") > col("e2.salary")) \
    .select(col("e1.name").alias("Employee"))

# Show results
result_df.show()
employee_df.createOrReplaceTempView("employee")
spark.sql("select e1.name as employee from employee e1 join employee e2 on e1.managerId = e2.id where e1.salary >e2.salary").show()
