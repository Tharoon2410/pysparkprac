from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark
spark = SparkSession.builder.appName("CandidateBudgetFilter").getOrCreate()

# Create sample data
data = [
    (1, 'Junior', 10000),
    (2, 'Junior', 15000),
    (3, 'Junior', 40000),
    (4, 'Senior', 16000),
    (5, 'Senior', 20000),
    (6, 'Senior', 50000)
]

columns = ["emp_id", "experience", "salary"]

df = spark.createDataFrame(data, columns)

# Define window for running salary
window_spec = Window.partitionBy("experience").orderBy("salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Add running salary column
df_with_running = df.withColumn("running_sal", sum("salary").over(window_spec))

seniors = df_with_running.filter((df_with_running.experience == "Senior") & (df_with_running.running_sal <= 70000))
# Total salary of selected seniors
senior_total_salary = seniors.agg(sum("salary")).collect()[0][0]  # 16000 + 20000 = 36000

# Remaining budget for juniors
junior_budget = 70000 - senior_total_salary
juniors = df_with_running.filter((df_with_running.experience == "Junior") & (df_with_running.running_sal <= junior_budget))
final_df = juniors.unionByName(seniors)

# Show final output
final_df.orderBy("emp_id").show()


