#WITH cte AS (SELECT *,CASE WHEN type = 'Deposit' Then amount ELSE -1*amount END AS amount_with_sign from Transactions)
#select account_id,day,SUM(amount_with_sign)over(PARTITION BY account_id ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS balance from cte ORDER BY account_id, day

# Define the window specification
#window_spec = Window.partitionBy("group").orderBy("value") \
#    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
	
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Step 1: Initialize Spark session
spark = SparkSession.builder.appName("TransactionsExample").getOrCreate()

# Step 2: Define schema using StructType
schema = StructType([
    StructField("account_id", IntegerType(), True),
    StructField("day", StringType(), True),      # or use DateType() and convert later
    StructField("type", StringType(), True),
    StructField("amount", IntegerType(), True)
])

# Step 3: Create data (as a list of tuples)
data = [
    (1, "2021-11-07", "Deposit", 2000),
    (1, "2021-11-09", "Withdraw", 1000),
    (1, "2021-11-11", "Deposit", 3000),
    (2, "2021-12-07", "Deposit", 7000),
    (2, "2021-12-12", "Withdraw", 7000)
]

# Step 4: Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# (Optional) Convert 'day' column to DateType

df = df.withColumn("day", to_date(col("day"), "yyyy-MM-dd"))

# Step 5: Show the DataFrame
df.show()
window_spec = Window.partitionBy("account_id").orderBy("day") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

sum_df = df.withColumn("amount_with_sign",expr("""CASE WHEN type ='Deposit' THEN amount ELSE -1*amount END"""))\
.withColumn("balance",sum("amount_with_sign").over(window_spec)).orderBy("account_id","day")

final_df = sum_df.select("account_id","day","balance")
final_df.show()

df.createOrReplaceTempView("transactions")
spark.sql("""WITH cte AS (SELECT *,CASE WHEN type = 'Deposit' Then amount ELSE -1*amount END AS amount_with_sign from Transactions)
select account_id,day,SUM(amount_with_sign)over(PARTITION BY account_id ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS balance from cte ORDER BY account_id, day""").show()


