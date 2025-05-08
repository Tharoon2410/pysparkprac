from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create Spark session
spark = SparkSession.builder.appName("EmployeeData").getOrCreate()

# Define schema
schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("emp_name", StringType(), True),
    StructField("department_id", IntegerType(), True),
    StructField("salary", IntegerType(), True),
    StructField("manager_id", IntegerType(), True),
    StructField("emp_age", IntegerType(), True)
])

# Create data as a list of tuples
data = [
    (1, 'Ankit', 100, 10000, 4, 39),
    (2, 'Mohit', 100, 15000, 5, 48),
    (3, 'Vikas', 100, 12000, 4, 37),
    (4, 'Rohit', 100, 14000, 2, 16),
    (5, 'Mudit', 200, 20000, 6, 55),
    (6, 'Agam', 200, 12000, 2, 14),
    (7, 'Sanjay', 200, 9000, 2, 13),
    (8, 'Ashish', 200, 5000, 2, 12),
    (9, 'Mukesh', 300, 6000, 6, 51),
    (10, 'Rakesh', 500, 7000, 6, 50)
]

# Create DataFrame
emp_df = spark.createDataFrame(data, schema)

# Show the DataFrame
emp_df.show()

df = emp_df.alias("df")
df1 = emp_df.alias("df1")
df2 = emp_df.alias("df2")
selfjoin_df = df.join(df1,col("df.manager_id") == col("df1.emp_id"),"left").join(df2,col("df1.manager_id") == col("df2.emp_id"),"left")

resultdf = selfjoin_df.selectExpr("df.emp_id","df.emp_name","df1.emp_name as manager_name","df2.emp_name as seniormanager")

resultdf.show()

emp_df.createOrReplaceTempView("emp")
spark.sql("""select e.emp_id,e.emp_name,m.emp_name as manager_name,sm.emp_name as senior_manager from emp e left join emp m on e.manager_id=m.emp_id left join emp sm on m.manager_id=sm.emp_id""").show()