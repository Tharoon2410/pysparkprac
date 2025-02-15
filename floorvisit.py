from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize SparkSession
spark = SparkSession.builder.appName("StringAggregationExample").getOrCreate()

# Sample data
data = [
    ("A", "Bangalore", "A@gmail.com", 1, "CPU"),
    ("A", "Bangalore", "A1@gmail.com", 1, "CPU"),
    ("A", "Bangalore", "A2@gmail.com", 2, "DESKTOP"),
    ("B", "Bangalore", "B@gmail.com", 2, "DESKTOP"),
    ("B", "Bangalore", "B1@gmail.com", 2, "DESKTOP"),
    ("B", "Bangalore", "B2@gmail.com", 1, "MONITOR")
]

# Define the schema (columns)
columns = ["name", "address", "email", "floor", "resources"]

# Create the DataFrame
prac_entries = spark.createDataFrame(data, columns)

# Show the DataFrame
prac_entries.show()

# 1. distinct_resources: Get distinct name, resources
distinct_resources = prac_entries.select("name", "resources").distinct()

# 2. agg_resources: Aggregate resources for each name
agg_resources = distinct_resources.groupBy("name").agg(
    F.concat_ws(",", F.collect_list("resources")).alias("used_resources")
)

# 3. total_visits: Count the total visits and aggregate resources for each name
total_visits = prac_entries.groupBy("name").agg(
    F.count("*").alias("total_visits"),
    F.concat_ws(",", F.collect_list("resources")).alias("resources_used")
)

# 4. floor_visit: Count floor visits and assign rank based on the number of visits
floor_visit_count = prac_entries.groupBy("name", "floor").agg(
    F.count("*").alias("no_of_floor_visit")
)

# Now apply rank using window function based on the count of floor visits
windowSpec = Window.partitionBy("name").orderBy(F.desc("no_of_floor_visit"))
floor_visit = floor_visit_count.withColumn(
    "rn", F.rank().over(windowSpec)
)

# 5. Join the DataFrames
result = floor_visit.join(total_visits, "name", "inner") \
    .join(agg_resources, "name", "inner") \
    .filter(floor_visit.rn == 1) \
    .select(
        floor_visit.name,
        floor_visit.floor.alias("most_visited_floor"),
        total_visits.total_visits,
        # Clean up the used_resources column by removing extra spaces and commas
        F.trim(F.regexp_replace(agg_resources.used_resources, ",+", ",")).alias("used_resources")
    )

# Show the result
result.show()

===================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

#initialize spark session
spark = SparkSession.builder.appName("StringAggregationExample").getOrCreate()

# Sample data
data = [
    ("A", "Bangalore", "A@gmail.com", 1, "CPU"),
    ("A", "Bangalore", "A1@gmail.com", 1, "CPU"),
    ("A", "Bangalore", "A2@gmail.com", 2, "DESKTOP"),
    ("B", "Bangalore", "B@gmail.com", 2, "DESKTOP"),
    ("B", "Bangalore", "B1@gmail.com", 2, "DESKTOP"),
    ("B", "Bangalore", "B2@gmail.com", 1, "MONITOR")
]

# Define the schema (columns)
columns = ["name","address","email","floor","resources"]

# Create the DataFrame
prac_entries = spark.createDataFrame(data, columns)

# Show the DataFrame
prac_entries.show()
# 1. distinct_resources: Get distinct name, resources
distinct_resources = prac_entries.select("name","resources").distinct()
distinct_resources.show()

# 2. agg_resources: Aggregate resources for each name
agg_resources = distinct_resources.groupBy("name").agg(
F.concat_ws(",",F.collect_list("resources")).alias("used_resources")
)
agg_resources.show()
# 3. total_visits: Count the total visits and aggregate resources for each name
total_visits = prac_entries.groupBy("name").agg(
F.count("*").alias("total_visits"),
F.concat_ws(",",F.collect_list("resources")).alias("resources_used")
)
total_visits.show()

# 4. floor_visit: Count floor visits and assign rank based on the number of visits
floor_visit_count = prac_entries.groupBy("name","floor").agg(
F.count("*").alias("no_of_floor_visit")
)
floor_visit_count.show()

#Now apply rank using window function based on the count of floor visits
windowSpec = Window.partitionBy("name").orderBy(F.desc("no_of_floor_visit"))
floor_visit = floor_visit_count.withColumn(
"rn",F.rank().over(windowSpec)
)
floor_visit.show()

# 5. Join the DataFrames
result = floor_visit.join(total_visits,"name","inner")\
.join(agg_resources,"name","inner")\
.filter(floor_visit.rn == 1)\
.select(
   floor_visit.name,
   floor_visit.floor.alias("most_visited_floor"),
   total_visits.total_visits,
   F.trim(F.regexp_replace(agg_resources.used_resources,",+",",")).alias("used_resources")
)

# Show the result
result.show()
