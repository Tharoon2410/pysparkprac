from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max

# Initialize SparkSession
spark = SparkSession.builder.appName("QuietStudents").getOrCreate()

# Data for students
students_data = [
    (1, 'Daniel'),
    (2, 'Jade'),
    (3, 'Stella'),
    (4, 'Jonathan'),
    (5, 'Will')
]

# Data for exams
exams_data = [
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

# Create schemas
students_schema = ["student_id", "student_name"]
exams_schema = ["exam_id", "student_id", "score"]

# Create DataFrames
students_df = spark.createDataFrame(students_data, schema=students_schema)
exams_df = spark.createDataFrame(exams_data, schema=exams_schema)

# Compute min and max scores for each exam
min_max_scores = exams_df.groupBy("exam_id").agg(
    min("score").alias("min_score"),
    max("score").alias("max_score")
)

# Join the exam data with min and max scores
joined_df = exams_df.join(min_max_scores, "exam_id")

# Identify non-"quiet" students (those who scored either the high score or the low score)
non_quiet_df = joined_df.filter(
    (col("score") == col("min_score")) | (col("score") == col("max_score"))
).select("student_id").distinct()

# Identify "quiet" students by excluding non-"quiet" students
quiet_students_df = students_df.join(
    exams_df.select("student_id").distinct(),
    "student_id"
).join(
    non_quiet_df,
    students_df["student_id"] == non_quiet_df["student_id"],
    "left_anti"
)

# Show the result
quiet_students_df.select("student_id", "student_name").show()

exams_df.createOrReplaceTempView("exams")
students_df.createOrReplaceTempView("students")
spark.sql("WITH min_max_scores AS (\
SELECT \
exam_id,\
MIN(score) AS min_score,\
MAX(score) AS max_score \
FROM exams \
GROUP BY exam_id\
),\
non_quiet_students AS (\
    SELECT DISTINCT \
e.student_id \
FROM exams e \
INNER JOIN min_max_scores mms \
ON e.exam_id = mms.exam_id \
WHERE e.score = mms.min_score OR e.score = mms.max_score\
),\
all_exam_students AS (\
    SELECT DISTINCT \
student_id \
FROM exams \
) \
SELECT \
s.student_id,\
s.student_name \
FROM students s \
INNER JOIN all_exam_students aes \
ON s.student_id = aes.student_id \
LEFT JOIN non_quiet_students nqs \
ON s.student_id = nqs.student_id \
WHERE nqs.student_id IS NULL;").show()