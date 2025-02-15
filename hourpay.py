from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("wages").getOrCreate()

data=[
('Sachin','1990-01-01',25),
('Sehwag' ,'1989-01-01', 15),
('Dhoni' ,'1989-01-01', 20),
('Sachin' ,'1991-02-05', 30)]
columns=["emp_name",
"bill_date",
"bill_rate"]
data1=[('Sachin', '1990-07-01' ,3),
('Sachin', '1990-07-01', 5),
('Sehwag','1990-07-01', 2),
('Sachin','1991-07-01', 4)]
columns1=["emp_name","work_date","bill_hrs"]
billing_df = spark.createDataFrame(data,columns)

hoursworked_df=spark.createDataFrame(data1,columns1)


billing_df=billing_df.withColumn("bill_date",to_date("bill_date","yyyy-mm-dd"))
billing_df.show()
hoursworked_df=hoursworked_df.withColumn("work_date",to_date("work_date","yyyy-mm-dd"))
hoursworked_df.show()
# Step 1: Create Window Specification to get the next bill_date
window_spec = Window.partitionBy("emp_name").orderBy("bill_date")

# Step 2: Create the "next_bill_date" column
date_range_df = billing_df.withColumn(
    "next_bill_date",
    coalesce(lead("bill_date", 1).over(window_spec), to_date(lit("1999-12-31"), "yyyy-MM-dd"))
)

# Step 3: Perform the RIGHT JOIN with hoursworked_df
join_df = hoursworked_df.join(
    date_range_df,
    hoursworked_df.emp_name == date_range_df.emp_name,
    "left"  # RIGHT JOIN in PySpark
).drop(date_range_df.emp_name)

# Step 4: Calculate total_charges using SQL CASE WHEN statement inside expr()
result_df = join_df.withColumn(
    "total_charges",
    expr("""
        CASE 
            WHEN work_date BETWEEN bill_date AND next_bill_date THEN bill_rate * bill_hrs
            ELSE 0 
        END
    """)
)

# Step 5: Aggregate the results by emp_name to get the total charges
aggregated_df = result_df.groupBy("emp_name").agg(
    sum("total_charges").alias("total_charges")
)

# Show the result
aggregated_df.show(truncate=False)