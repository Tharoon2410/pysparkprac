from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Step 1: Create Spark Session
spark = SparkSession.builder.appName("SalaryIncreases").getOrCreate()

# Step 2: Define Schema
schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("salary", IntegerType(), True),
    StructField("year", IntegerType(), True)
])

# Step 3: Create Data
data = [
    (1, 60000, 2018),
    (1, 70000, 2019),
    (1, 80000, 2020),
    (2, 60000, 2018),
    (2, 65000, 2019),
    (2, 65000, 2020),
    (3, 60000, 2018),
    (3, 65000, 2019)
]

# Step 4: Create DataFrame
employee_df = spark.createDataFrame(data, schema=schema)

# Step 5: Show DataFrame
employee_df.show()
winspec = Window.partitionBy("emp_id").orderBy("year")
data_df = employee_df.withColumn("lagsalary",lag("salary").over("winspec"))\
.withColumn("increase_salary",diff("lagsalary-salary"))
finaldf = data_df.select("emp_id","salary","year","increase_salary")
finaldf.show()

# Step 5: Show DataFrame
employee_df.show()
winspec = Window.partitionBy("emp_id").orderBy("year")
# Pass the 'winspec' object to the over method, not the string "winspec"
data_df = employee_df.withColumn("lagsalary",lag("salary", 1).over(winspec))\
.withColumn("increase_salary",expr("salary - lagsalary"))
finaldf = data_df.select("emp_id","salary","year","increase_salary")
finaldf.show()


####################
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


# Initialize Spark session (if not already done)
spark = SparkSession.builder.appName("transaction").getOrCreate()

# Define the schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("trans_date", StringType(), True)
])

# Define the data
data = [
    (121, "US", "approved", 1000, "2018-12-18"),
    (122, "US", "declined", 2000, "2018-12-19"),
    (123, "US", "approved", 2000, "2019-01-01"),
    (124, "DE", "approved", 2000, "2019-01-07")
]

# Create the DataFrame
transactions = spark.createDataFrame(data, schema=schema)
transactions = transactions.withColumn("trans_date", to_date(col("trans_date"), "yyyy-MM-dd"))

# Show the DataFrame
transactions.show()
# Add the 'month' column
transactions_with_month = transactions.withColumn(
    "month", date_format(col("trans_date"), "yyyy-MM")
)

# Group by month and country and aggregate
result = transactions_with_month.groupBy("month", "country").agg(
    count("*").alias("trans_count"),
    sum(when(col("state") == "approved", 1).otherwise(0)).alias("approved_count"),
    sum(col("amount")).alias("trans_total_amount"),
    sum(when(col("state") == "approved", col("amount")).otherwise(0)).alias("approved_total_amount")
).orderBy("month", "country")

# Show the result
result.show()
transactions.createOrReplaceTempView("Transactions")
spark.sql("select date_format(trans_date,'yyyy-MM') as month,country,count(state) as trans_count,sum(amount) as trans_total_amount,sum(case when state = 'approved' then 1 else 0 end) as approved_count,sum(case when state = 'approved' then amount else 0 end) as approved_total_amount from transactions group by date_format(trans_date,'yyyy-MM'),country order by country desc,trans_count desc").show()