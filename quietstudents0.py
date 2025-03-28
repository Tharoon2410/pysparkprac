from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, min, max

# Initialize SparkSession
spark = SparkSession.builder.appName("QueryConversion").getOrCreate()

# Sample data
data = [
    (10, 1, 70),
    (10, 2, 80),
    (10, 3, 90),
    (20, 1, 80),
    (30, 1, 70),
    (30, 3, 80),
    (30, 4, 90),
    (40, 1, 60),
    (40, 2, 70),
    (40, 4, 80)
]

# Schema
schema = ["exam_id", "student_id", "score"]

# Create DataFrame
exam_df = spark.createDataFrame(data, schema=schema)

# Compute min and max scores for each exam_id
all_scores = exam_df.groupBy("exam_id").agg(
    min("score").alias("min_score"),
    max("score").alias("max_score")
)

# Join exam_df with all_scores DataFrame on exam_id
joined_df = exam_df.join(all_scores, "exam_id", "inner")

# Compute the red_flag column using expr with CASE WHEN
result_df = joined_df.withColumn(
    "red_flag",
    expr("CASE WHEN score = min_score OR score = max_score THEN 1 ELSE 0 END")
).groupBy("student_id").agg(
    max("red_flag").alias("red_flag")
).orderBy("student_id")

# Show the final result
result_df.show()
exam_df.createOrReplaceTempView("exam")
spark.sql("with all_scores as(select exam_id,min(score) as min_score,max(score) as max_score from exam group by exam_id)\
select exam.student_id,max(case when score = min_score or score = max_score then 1 else 0 end)red_flag from exam inner join all_scores on exam.exam_id=all_scores.exam_id group by student_id order by student_id").show()