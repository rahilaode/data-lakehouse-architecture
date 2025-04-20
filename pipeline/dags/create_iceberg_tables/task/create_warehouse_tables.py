from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, to_date, date_format, year, month, quarter, dayofyear,
    date_trunc, when, sequence, lit, explode, dayofmonth, date_add, weekofyear,
    hour, minute
)

# === Setup SparkSession
AWS_ACCESS_KEY_ID = "minio"
AWS_SECRET_ACCESS_KEY = "minio123"

spark = SparkSession.builder \
    .appName("Create Final Iceberg Tables and Populate Dim Tables") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "hive") \
    .config("spark.sql.catalog.demo.uri", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalog.demo.warehouse", "s3a://hive/warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS warehouse;
""")



# # === Generate dim_date data
# start_date = "1998-01-01"
# end_date = "2078-01-01"

# date_df = spark.sql(f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS date_actual")

# dim_date_df = date_df \
#     .withColumn("date_id", date_format("date_actual", "yyyyMMdd").cast("int")) \
#     .withColumn("day_suffix", expr("date_format(date_actual, 'd') || case when date_format(date_actual, 'd') in ('1','21','31') then 'st' when date_format(date_actual, 'd') in ('2','22') then 'nd' when date_format(date_actual, 'd') in ('3','23') then 'rd' else 'th' end")) \
#     .withColumn("day_name", date_format("date_actual", "EEEE")) \
#     .withColumn("day_of_year", dayofyear("date_actual")) \
#     .withColumn("week_of_month", expr("ceil(day(date_actual)/7)")) \
#     .withColumn("week_of_year", weekofyear("date_actual")) \
#     .withColumn("week_of_year_iso", expr("year(date_actual) || '-W' || lpad(weekofyear(date_actual), 2, '0')")) \
#     .withColumn("month_actual", month("date_actual")) \
#     .withColumn("month_name", date_format("date_actual", "MMMM")) \
#     .withColumn("month_name_abbreviated", date_format("date_actual", "MMM")) \
#     .withColumn("quarter_actual", quarter("date_actual")) \
#     .withColumn("quarter_name", expr("CASE quarter(date_actual) WHEN 1 THEN 'First' WHEN 2 THEN 'Second' WHEN 3 THEN 'Third' ELSE 'Fourth' END")) \
#     .withColumn("year_actual", year("date_actual")) \
#     .withColumn("first_day_of_week", expr("date_add(date_actual, -(CAST(date_format(date_actual, 'u') AS INT) - 1))")) \
#     .withColumn("last_day_of_week", expr("date_add(date_actual, 7 - CAST(date_format(date_actual, 'u') AS INT))")) \
#     .withColumn("first_day_of_month", expr("trunc(date_actual, 'MM')")) \
#     .withColumn("last_day_of_month", expr("last_day(date_actual)")) \
#     .withColumn("first_day_of_quarter", expr("trunc(date_actual, 'quarter')")) \
#     .withColumn("last_day_of_quarter", expr("add_months(trunc(date_actual, 'quarter'), 3) - interval 1 day")) \
#     .withColumn("first_day_of_year", expr("trunc(date_actual, 'year')")) \
#     .withColumn("last_day_of_year", expr("add_months(trunc(date_actual, 'year'), 12) - interval 1 day")) \
#     .withColumn("mmyyyy", date_format("date_actual", "MMyyyy")) \
#     .withColumn("mmddyyyy", date_format("date_actual", "MMddyyyy")) \
#     .withColumn("weekend_indr", when(expr("date_format(date_actual, 'u')").isin("6", "7"), "weekend").otherwise("weekday"))

# # === Write to Iceberg dim_date
# dim_date_df.writeTo("warehouse.final_dim_date").overwritePartitions()

# # === Generate dim_time data
# minute_df = spark.sql("SELECT sequence(to_timestamp('00:00', 'HH:mm'), to_timestamp('23:59', 'HH:mm'), interval 1 minute) AS minute_list") \
#     .select(explode(col("minute_list")).alias("minute"))

# dim_time_df = minute_df \
#     .withColumn("time_id", date_format("minute", "HHmm").cast("int")) \
#     .withColumn("time_actual", date_format("minute", "HH:mm")) \
#     .withColumn("hours_24", date_format("minute", "HH")) \
#     .withColumn("hours_12", date_format("minute", "hh")) \
#     .withColumn("hour_minutes", date_format("minute", "mm")) \
#     .withColumn("day_minutes", expr("hour(minute) * 60 + minute(minute)")) \
#     .withColumn("day_time_name", when(col("time_actual") <= "11:59", "AM").otherwise("PM")) \
#     .withColumn("day_night", when(col("time_actual").between("07:00", "19:59"), "Day").otherwise("Night"))

# # === Write to Iceberg dim_time
# dim_time_df.writeTo("warehouse.final_dim_time").overwritePartitions()

# Done!
spark.stop()