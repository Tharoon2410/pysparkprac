from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("worldcup").getOrCreate()

df = spark.read.format("csv").option("header",True).load("/content/sample_data/icc_worldcup.csv")
df.show()
df1 = df.withColumn("win_flag",expr("CASE WHEN team_1 = winner THEN 1 ELSE 0 END")).selectExpr("team_1 as team_name","win_flag")
df2 = df.withColumn("win_flag",expr("CASE WHEN team_2 = winner THEN 1 ELSE 0 END")).selectExpr("team_2 as team_name","win_flag")
df_union = df1.union(df2)
result_df = df_union.groupBy("team_name").agg( count("*").alias("no_of_matches_played"),
        sum("win_flag").alias("no_of_matches_won"),
        (count("*") - sum("win_flag")).alias("no_of_losses")
    ).orderBy(desc("no_of_matches_won"))


# Display result
result_df.show()