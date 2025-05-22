from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *

# Create Spark session
spark = SparkSession.builder.appName("StudentExams").getOrCreate()

# Define schemas
students_schema = StructType([
    StructField("student_id", IntegerType(), True),
    StructField("student_name", StringType(), True)
])

subjects_schema = StructType([
    StructField("subject_name", StringType(), True)
])

examinations_schema = StructType([
    StructField("student_id", IntegerType(), True),
    StructField("subject_name", StringType(), True)
])

# Create data
students_data = [
    (1, "Alice"),
    (2, "Bob"),
    (13, "John"),
    (6, "Alex")
]

subjects_data = [
    ("Math",),
    ("Physics",),
    ("Programming",)
]

examinations_data = [
    (1, "Math"),
    (1, "Physics"),
    (1, "Programming"),
    (2, "Programming"),
    (1, "Physics"),
    (1, "Math"),
    (13, "Math"),
    (13, "Programming"),
    (13, "Physics"),
    (2, "Math"),
    (1, "Math")
]

# Create DataFrames
students_df = spark.createDataFrame(data=students_data, schema=students_schema)
subjects_df = spark.createDataFrame(data=subjects_data, schema=subjects_schema)
exams_df = spark.createDataFrame(data=examinations_data, schema=examinations_schema)

# Show DataFrames
students_df.show()
subjects_df.show()
exams_df.show()

# CROSS JOIN Students x Subjects (same as joining without a condition to get all student-subject combinations)
student_subject_df = students_df.crossJoin(subjects_df)

# LEFT JOIN with Examinations on (student_id, subject_name)
#joined_df = student_subject_df.join(
#    exams_df.alias("e"),
#    on=["student_id", "subject_name"],
#    how="left"
#)


# LEFT JOIN with examinations_df on (student_id AND subject_name)
joined_df = student_subject_df.alias('ss').join(
    exams_df.alias('e'),
    (col("ss.student_id") == col("e.student_id")) &
    (col("ss.subject_name") == col("e.subject_name")),
    how="left"
)
# GROUP BY student_id, subject_name, and count attended exams
result_df = joined_df.groupBy("ss.student_id", "ss.student_name", "ss.subject_name") \
    .agg(count("e.student_id").alias("attended_exams")) \
    .orderBy("ss.student_id", "ss.subject_name")


# Show the result
result_df.show()
students_df.createOrReplaceTempView("students")
subjects_df.createOrReplaceTempView("subjects")
exams_df.createOrReplaceTempView("exams")
spark.sql("""SELECT
  Students.student_id,
  Students.student_name,
  Subjects.subject_name,
  COUNT(Examinations.student_id) AS attended_exams
FROM Students
CROSS JOIN Subjects
LEFT JOIN Examinations
  ON (
    Students.student_id = Examinations.student_id
    AND Subjects.subject_name = Examinations.subject_name)
GROUP BY 1, 2, 3
ORDER BY Students.student_id, Subjects.subject_name""").show()