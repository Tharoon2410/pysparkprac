'''WITH txn_summary AS (
    SELECT
        CUST_ID,
        COUNT(DISTINCT TXN_DT) AS TRANS_FREQUENCY,
        SUM(TXN_AMT) AS TOTAL_TRANSAMT,
        AVG(TXN_AMT) AS AVG_TRANSAMT
    FROM transactions
    GROUP BY CUST_ID
),
filtered_customers AS (
    SELECT *
    FROM txn_summary
    WHERE TRANS_FREQUENCY > 1
      AND AVG_TRANSAMT > 1000
)
SELECT
    CUST_ID,
    TRANS_FREQUENCY,
    TOTAL_TRANSAMT,
    DENSE_RANK() OVER (ORDER BY TOTAL_TRANSAMT DESC) AS RANKING
FROM filtered_customers;'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, avg, sum as _sum, dense_rank
from pyspark.sql.window import Window

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Read CSV (you can change the path)
df = spark.read.option("header", True).option("inferSchema", True).csv("path/to/txn.csv")

# Step 1: Convert TXN_DT to date format if needed
from pyspark.sql.functions import to_date
df = df.withColumn("TXN_DT", to_date("TXN_DT", "d-MMM-yy"))

# Step 2: Aggregate by CUST_ID
agg_df = df.groupBy("CUST_ID").agg(
    countDistinct("TXN_DT").alias("TRANS_FREQUENCY"),
    _sum("TXN_AMT").alias("TOTAL_TRANSAMT"),
    avg("TXN_AMT").alias("AVG_TRANSAMT")
)

# Step 3: Filter conditions
filtered_df = agg_df.filter(
    (col("TRANS_FREQUENCY") > 1) & 
    (col("AVG_TRANSAMT") > 1000)
)

# Step 4: Ranking based on TOTAL_TRANSAMT
windowSpec = Window.orderBy(col("TOTAL_TRANSAMT").desc())

ranked_df = filtered_df.withColumn("RANKING", dense_rank().over(windowSpec)) \
                       .select("CUST_ID", "TRANS_FREQUENCY", "TOTAL_TRANSAMT", "RANKING")

# Show result
ranked_df.show()
