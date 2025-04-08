###########
##leet1783##
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder.master("local[1]").appName("Tennis Championships").getOrCreate()

# Define the schema for Players and Championships tables
players_schema = StructType([
    StructField("player_id", IntegerType(), False),
    StructField("player_name", StringType(), False)
])

championships_schema = StructType([
    StructField("year", IntegerType(), False),
    StructField("Wimbledon", IntegerType(), False),
    StructField("Fr_open", IntegerType(), False),
    StructField("US_open", IntegerType(), False),
    StructField("Au_open", IntegerType(), False)
])

# Create the DataFrame for Players table
players_data = [(1, 'Nadal'),
                (2, 'Federer'),
                (3, 'Novak')]

players_df = spark.createDataFrame(players_data, schema=players_schema)

# Create the DataFrame for Championships table
championships_data = [(2018, 1, 1, 1, 1),
                      (2019, 1, 1, 2, 2),
                      (2020, 2, 1, 2, 2)]

championships_df = spark.createDataFrame(championships_data, schema=championships_schema)

# Show the DataFrames to check
players_df.show()
championships_df.show()
# 1. Prepare the subqueries (count wins per tournament for each player)
wimbledon_wins = championships_df.groupBy("Wimbledon").agg(count("*").alias("wins")).withColumnRenamed("Wimbledon", "p_id")
fr_open_wins = championships_df.groupBy("Fr_open").agg(count("*").alias("wins")).withColumnRenamed("Fr_open", "p_id")
us_open_wins = championships_df.groupBy("US_open").agg(count("*").alias("wins")).withColumnRenamed("US_open", "p_id")
au_open_wins = championships_df.groupBy("Au_open").agg(count("*").alias("wins")).withColumnRenamed("Au_open", "p_id")

# 2. Union all subqueries to combine the data
all_wins = wimbledon_wins.union(fr_open_wins).union(us_open_wins).union(au_open_wins)

# 3. Join with Players DataFrame to get player names
joined_df = all_wins.join(players_df, all_wins.p_id == players_df.player_id, how='inner').select(
    players_df.player_id.alias("player_id"),
    players_df.player_name,
    "wins"
)

# 4. Aggregate the total wins (grand slams count) per player
result_df = joined_df.groupBy("player_id", "player_name").agg(
    sum("wins").alias("grand_slams_count")
)

# Show the result
result_df.show()
players_df.createOrReplaceTempView("players")
championships_df.createOrReplaceTempView("championships")
spark.sql("select p_id as player_id, player_name, sum(wins) as  grand_slams_count from \
(select wimbledon as p_id,count(*) as wins from Championships \
group by 1 \
union all \
select Fr_open as p_id,count(*) as wins from Championships \
group by 1 \
union all \
select US_open as p_id,count(*) as wins from Championships \
group by 1 \
union all \
select Au_open as p_id,count(*) as wins from Championships \
group by 1 \
) X \
inner join Players Y  on X.p_id = Y.player_id \
group by 1,2").show()
#########################
##leet 1393##
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *

# Initialize Spark Session
spark = SparkSession.builder.appName('StockOperations').getOrCreate()

# Define schema
schema = StructType([
    StructField("stock_name", StringType(), True),
    StructField("operation", StringType(), True),
    StructField("operation_day", IntegerType(), True),
    StructField("price", IntegerType(), True)
])

# Data (in the form of a list of tuples)
data = [
    ('Leetcode', 'Buy', 1, 1000),
    ('Corona Masks', 'Buy', 2, 10),
    ('Leetcode', 'Sell', 5, 9000),
    ('Handbags', 'Buy', 17, 30000),
    ('Corona Masks', 'Sell', 3, 1010),
    ('Corona Masks', 'Buy', 4, 1000),
    ('Corona Masks', 'Sell', 5, 500),
    ('Corona Masks', 'Buy', 6, 1000),
    ('Handbags', 'Sell', 29, 7000),
    ('Corona Masks', 'Sell', 10, 10000)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()
# Calculate capital gain/loss using the CASE logic (with when)
df_with_capital_gain_loss = df.withColumn(
    "capital_gain_loss",
    when(col("operation") == "Buy", col("price") * -1).otherwise(col("price"))
    )
capital_gain_loss = df_with_capital_gain_loss.groupBy("stock_name").agg(
    sum("capital_gain_loss").alias("capital_gain_loss")
)
finaldf = capital_gain_loss.orderBy(col("capital_gain_loss").desc()).select("stock_name","capital_gain_loss")
finaldf.show()
#exprdf = df.selectExpr("*","case when operation = 'buy' then price*-1 else price end as capital_gain_loss")
#exprdf.show()
# Stop the Spark session
df.createOrReplaceTempView("stocks")
spark.sql("select stock_name,sum(case when operation = 'Buy' then price*-1 ELSE price end)AS capital_gain_loss\
 from stocks group by stock_name order by capital_gain_loss desc;").show()
