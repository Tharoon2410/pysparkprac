/* write a SQL query to find the cancellation rate of requestd with unbanned users(both client and driver must not be banned) each day between "2013-10-01" and "2013-10-03".Round  cancellation Rate to two decimal points.
the cancellation rate is computed by dividing the number of canceled(by client or driver) requests with unbanned users by the total number of requests with unbanned users on that day */

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row

# Initialize Spark session
spark = SparkSession.builder.appName("CancelledTrips").getOrCreate()

# Sample data for 'users' and 'trips' (you can replace this with actual data loading)
# Users data (client and driver data)
users_data = [
    Row(users_id=1, name="User1", banned="No"),
    Row(users_id=2, name="User2", banned="No"),
    Row(users_id=3, name="User3", banned="Yes"),
    Row(users_id=4, name="User4", banned="No")
]

# Trips data (trips with client_id, driver_id, and status)
trips_data = [
    Row(id=1, client_id=1, driver_id=2, status="completed", request_at="2024-01-01"),
    Row(id=2, client_id=3, driver_id=4, status="cancelled_by_driver", request_at="2024-01-02"),
    Row(id=3, client_id=2, driver_id=1, status="cancelled_by_client", request_at="2024-01-03"),
    Row(id=4, client_id=4, driver_id=2, status="completed", request_at="2024-01-04")
]

# Create DataFrames
users_df = spark.createDataFrame(users_data)
trips_df = spark.createDataFrame(trips_data)

# Step 1: Join trips with clients and drivers
joined_df = trips_df.join(
    users_df.alias("client"), trips_df["client_id"] == F.col("client.users_id"), "inner"
).join(
    users_df.alias("driver"), trips_df["driver_id"] == F.col("driver.users_id"), "inner"
).filter(
    (F.col("client.banned") == "No") & (F.col("driver.banned") == "No")
)

# Step 2: Calculate cancelled trip count, total trip count, and cancellation percentage
result = joined_df.groupBy("request_at").agg(
    # Count of cancelled trips (status in 'cancelled_by_client' or 'cancelled_by_driver')
    F.count(
        F.when(
            F.col("status").isin("cancelled_by_client", "cancelled_by_driver"), 1
        ).otherwise(None)
    ).alias("cancelled_trip_count"),

    # Count of total trips
    F.count(F.lit(1)).alias("total_trips"),

    # Cancellation percentage (cancelled trips / total trips * 100)
    (F.count(
        F.when(
            F.col("status").isin("cancelled_by_client", "cancelled_by_driver"), 1
        ).otherwise(None)
    ) / F.count(F.lit(1)) * 100).alias("cancelled_percent")
)

# Step 3: Show the result
result.show()

===========================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import *  # Import all functions

# Initialize Spark session
spark = SparkSession.builder.appName("CancelledTrips").getOrCreate()

# Sample data for 'users' and 'trips' (you can replace this with actual data loading)
# Users data (client and driver data)
users_data = [
    Row(users_id=1, name="User1", banned="No"),
    Row(users_id=2, name="User2", banned="No"),
    Row(users_id=3, name="User3", banned="Yes"),
    Row(users_id=4, name="User4", banned="No")
]

# Trips data (trips with client_id, driver_id, and status)
trips_data = [
    Row(id=1, client_id=1, driver_id=2, status="completed", request_at="2024-01-01"),
    Row(id=2, client_id=3, driver_id=4, status="cancelled_by_driver", request_at="2024-01-02"),
    Row(id=3, client_id=2, driver_id=1, status="cancelled_by_client", request_at="2024-01-03"),
    Row(id=4, client_id=4, driver_id=2, status="completed", request_at="2024-01-04")
]

# Create DataFrames
users_df = spark.createDataFrame(users_data)
trips_df = spark.createDataFrame(trips_data)

# Step 1: Join trips with clients and drivers
joined_df = trips_df.join(
    users_df.alias("client"), trips_df["client_id"] == col("client.users_id"), "inner"
).join(
    users_df.alias("driver"), trips_df["driver_id"] == col("driver.users_id"), "inner"
).filter(
    (col("client.banned") == "No") & (col("driver.banned") == "No")
)

# Step 2: Calculate cancelled trip count, total trip count, and cancellation percentage using SQL CASE WHEN
result = joined_df.groupBy("request_at").agg(
    # Count of cancelled trips using SQL CASE WHEN
    countExpr("""
        CASE 
            WHEN status IN ('cancelled_by_client', 'cancelled_by_driver') THEN 1 
            ELSE NULL 
        END
    """).alias("cancelled_trip_count"),

    # Count of total trips
    count(lit(1)).alias("total_trips"),

    # Cancellation percentage using SQL CASE WHEN
    (countExpr("""
        CASE 
            WHEN status IN ('cancelled_by_client', 'cancelled_by_driver') THEN 1 
            ELSE NULL 
        END
    """) / count(lit(1)) * 100).alias("cancelled_percent")
)

# Step 3: Show the result
result.show()

