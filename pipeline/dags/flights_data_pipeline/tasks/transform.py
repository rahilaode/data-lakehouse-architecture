import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp,lpad, when, concat_ws, from_json, lit
from pyspark.sql.types import StringType, StructType, IntegerType
from pyspark.sql import functions as F
import time

import datetime

AWS_ACCESS_KEY_ID = "minio"
AWS_SECRET_ACCESS_KEY = "minio123"

def transform(minio_ip, hive_metastore_ip):
    MINIO_IP = minio_ip
    HIVE_METASTORE_IP = hive_metastore_ip
    spark = SparkSession.builder \
        .appName("Transform DWH - Airlines Booking") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.demo.type", "hive") \
        .config("spark.sql.catalog.demo.uri", f"thrift://{HIVE_METASTORE_IP}:9083") \
        .config("spark.sql.catalog.demo.warehouse", "s3a://hive/warehouse/") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_IP}:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

    # reduce noise
    start = time.time()
    ticket_flights = spark.read.table("demo.default.ticket_flights")
    end = time.time()
    print(f"Read durations for ticket_flights table: {end - start} seconds")

    print("-->>>> START TRANSFORM <<<<--")
    start = time.time()
    print(ticket_flights.printSchema())
    end = time.time()
    print(f"Read durations for ticket_flights table: {end - start} seconds")
    
    start = time.time()
    ticket_flights.writeTo(f"demo.default.ticket_flights_transformed").overwritePartitions()
    end = time.time()
    print(f"Write durations for ticket_flights table to curated zone: {end - start} seconds")
    

    # aircrafts = spark.read.table("demo.default.aircrafts_data")
    # airports = spark.read.table("demo.default.airports_data")
    # tickets = spark.read.table("demo.default.tickets")
    # seats = spark.read.table("demo.default.seats")

    # # Dim airfcraft
    # aircrafts_schema = StructType() \
    #     .add("en", StringType()) \
    #     .add("ru", StringType()) 

    # aircrafts_transformed = aircrafts.withColumn(
    #     "model_parsed", from_json(col("model"), aircrafts_schema)
    # ).select(
    #     col("aircraft_code").alias("aircraft_id"),
    #     col("aircraft_code").alias("aircraft_nk"),
    #     col("model_parsed.en").alias("model_en"),
    #     col("model_parsed.ru").alias("model_ru"),
    #     col("range").alias("range"),
    #     current_timestamp().alias("created_at"),
    #     current_timestamp().alias("updated_at")
    # )

    # print(aircrafts_transformed.show())
    # aircrafts_transformed.writeTo(f"demo.default.dim_aircraft").overwritePartitions()

    # # Dim airports
    # airports_name_schema = StructType() \
    #     .add("en", StringType()) \
    #     .add("ru", StringType())

    # airports_city_schema = StructType() \
    #     .add("en", StringType()) \
    #     .add("ru", StringType())

    # airports_transformed = airports.withColumn(
    #     "name_parsed", from_json(col("airport_name"), airports_name_schema)
    # ).withColumn(
    #     "city_parsed", from_json(col("city"), airports_city_schema)
    # ).select(
    #     col("airport_code").alias("airport_id"),
    #     col("airport_code").alias("airport_nk"),
    #     col("name_parsed.en").alias("airport_name_en"),
    #     col("name_parsed.ru").alias("airport_name_ru"),
    #     col("city_parsed.en").alias("city_en"),
    #     col("city_parsed.ru").alias("city_ru"),
    #     col("coordinates").alias("coordinates"),
    #     col("timezone").alias("timezone"),
    #     current_timestamp().alias("created_at"),
    #     current_timestamp().alias("updated_at")
    # )

    # print(airports_transformed.show())
    # airports_transformed.writeTo(f"demo.default.dim_airport").overwritePartitions()


    # # Dim passenger
    # print(tickets.show())
    # # Skema untuk kolom contact_data (bertipe JSON string)
    # contact_schema = StructType() \
    #     .add("phone", StringType()) \
    #     .add("email", StringType())

    # # Transformasi DataFrame
    # tickets_transformed = tickets.withColumn(
    #     "contact_parsed", from_json(col("contact_data"), contact_schema)
    # ).select(
    #     col("passenger_id").alias("passenger_id"),
    #     col("passenger_id").alias("passenger_nk"),
    #     col("passenger_name"),
    #     col("contact_parsed.phone").alias("phone"),
    #     col("contact_parsed.email").alias("email"),
    #     current_timestamp().alias("created_at"),
    #     current_timestamp().alias("updated_at")
    # )
    # print(tickets_transformed.show())
    # tickets_transformed.writeTo(f"demo.default.dim_passenger").overwritePartitions()


    # # Dim seats
    # seats_mapped = seats.alias("s").join(
    #     aircrafts_transformed.alias("da"),
    #     col("s.aircraft_code") == col("da.aircraft_nk"),
    #     how="inner"
    # ).select(
    #     concat_ws("_", col("s.aircraft_code"), col("s.seat_no")).alias("seat_id"),
    #     col("da.aircraft_id").alias("aircraft_id"),
    #     col("s.seat_no"),
    #     col("s.fare_conditions")
    # )

    # seats_final = seats_mapped.withColumn("created_at", current_timestamp()) \
    #                         .withColumn("updated_at", current_timestamp())


    # seats_final.writeTo(f"demo.default.dim_seat").overwritePartitions()

    # # Dim date
    # # 1. Generate tanggal dari 1998-01-01 sampai 80 tahun ke depan
    # start_date = datetime.date(1998, 1, 1)
    # num_days = 29219  # 80 tahun
    # dates = [start_date + datetime.timedelta(days=i) for i in range(num_days)]

    # # 2. Create DataFrame
    # dates_df = spark.createDataFrame([(d,) for d in dates], ["date_actual"])

    # # 3. Hitung iso_dayofweek manual
    # dates_df = dates_df.withColumn(
    #     "iso_dayofweek", 
    #     ((F.dayofweek("date_actual") + 5) % 7) + 1
    # )

    # # 4. Transformasi kolom-kolom kalender
    # dim_date = dates_df \
    #     .withColumn("date_id", F.date_format(F.col("date_actual"), "yyyyMMdd").cast(IntegerType())) \
    #     .withColumn("day_suffix", F.expr("""
    #         CASE 
    #             WHEN dayofmonth(date_actual) IN (1,21,31) THEN concat(dayofmonth(date_actual), 'st')
    #             WHEN dayofmonth(date_actual) IN (2,22) THEN concat(dayofmonth(date_actual), 'nd')
    #             WHEN dayofmonth(date_actual) IN (3,23) THEN concat(dayofmonth(date_actual), 'rd')
    #             ELSE concat(dayofmonth(date_actual), 'th')
    #         END
    #     """)) \
    #     .withColumn("day_name", F.date_format(F.col("date_actual"), "EEEE")) \
    #     .withColumn("day_of_year", F.dayofyear(F.col("date_actual"))) \
    #     .withColumn("week_of_month", F.expr("weekofyear(date_actual) - weekofyear(trunc(date_actual, 'month')) + 1")) \
    #     .withColumn("week_of_year", F.weekofyear(F.col("date_actual"))) \
    #     .withColumn(
    #         "week_of_year_iso",
    #         F.concat_ws(
    #             "-W",
    #             F.year("date_actual"),
    #             F.lpad(F.weekofyear("date_actual").cast("string"), 2, "0")
    #         )
    #     ) \
    #     .withColumn("month_actual", F.month(F.col("date_actual"))) \
    #     .withColumn("month_name", F.date_format(F.col("date_actual"), "MMMM")) \
    #     .withColumn("month_name_abbreviated", F.date_format(F.col("date_actual"), "MMM")) \
    #     .withColumn("quarter_actual", F.quarter(F.col("date_actual"))) \
    #     .withColumn("quarter_name", F.expr("""
    #         CASE 
    #             WHEN quarter(date_actual) = 1 THEN 'First'
    #             WHEN quarter(date_actual) = 2 THEN 'Second'
    #             WHEN quarter(date_actual) = 3 THEN 'Third'
    #             WHEN quarter(date_actual) = 4 THEN 'Fourth'
    #         END
    #     """)) \
    #     .withColumn("year_actual", F.year(F.col("date_actual"))) \
    #     .withColumn("first_day_of_week", F.expr("date_add(date_actual, 1 - iso_dayofweek)")) \
    #     .withColumn("last_day_of_week", F.expr("date_add(date_actual, 7 - iso_dayofweek)")) \
    #     .withColumn("first_day_of_month", F.trunc(F.col("date_actual"), "month")) \
    #     .withColumn("last_day_of_month", F.last_day(F.col("date_actual"))) \
    #     .withColumn("first_day_of_quarter", F.trunc(F.col("date_actual"), "quarter")) \
    #     .withColumn("last_day_of_quarter", F.expr("date_add(add_months(trunc(date_actual, 'quarter'), 3), -1)")) \
    #     .withColumn("first_day_of_year", F.expr("to_date(concat(year(date_actual), '-01-01'))")) \
    #     .withColumn("last_day_of_year", F.expr("to_date(concat(year(date_actual), '-12-31'))")) \
    #     .withColumn("mmyyyy", F.date_format(F.col("date_actual"), "MMyyyy")) \
    #     .withColumn("mmddyyyy", F.date_format(F.col("date_actual"), "MMddyyyy")) \
    #     .withColumn("weekend_indr", F.when(F.col("iso_dayofweek").isin(6, 7), "weekend").otherwise("weekday")) \
    #     .drop("iso_dayofweek")  # Sudah tidak perlu lagi, sudah dipakai

    # # 5. Show sample
    # dim_date.show(10, truncate=False)

    # # tampilkan
    # dim_date.show()

    # # 6. Write to Iceberg table
    # dim_date.writeTo(f"demo.default.dim_date").overwritePartitions()


    # # Dim time
    # # Generate 0-1439 (menit dalam sehari)
    # minutes = list(range(0, 24 * 60))  # 0..1439

    # # Create DataFrame
    # minutes_df = spark.createDataFrame([(i,) for i in minutes], ["minute_of_day"])

    # # Build dim_time
    # dim_time = minutes_df.withColumn(
    #     "hours_24",
    #     lpad(F.floor(col("minute_of_day") / 60).cast("string"), 2, "0")
    # ).withColumn(
    #     "hours_12",
    #     lpad(((F.floor(col("minute_of_day") / 60)) % 12).cast("string"), 2, "0")
    # ).withColumn(
    #     "hour_minutes",
    #     lpad((col("minute_of_day") % 60).cast("string"), 2, "0")
    # ).withColumn(
    #     "time_actual",
    #     concat_ws(":",
    #         lpad(F.floor(col("minute_of_day") / 60).cast("string"), 2, "0"),
    #         lpad((col("minute_of_day") % 60).cast("string"), 2, "0"),
    #         lit("00")   # Detik diset "00"
    #     )
    # ).withColumn(
    #     "day_minutes",
    #     col("minute_of_day")
    # ).withColumn(
    #     "day_time_name",
    #     when(col("minute_of_day") < 720, "AM").otherwise("PM")
    # ).withColumn(
    #     "day_night",
    #     when((col("minute_of_day") >= 420) & (col("minute_of_day") <= 1199), "Day")
    #     .otherwise("Night")
    # ).withColumn(
    #     "time_id",
    #     (lpad(F.floor(col("minute_of_day") / 60).cast("string"), 2, "0") +
    #     lpad((col("minute_of_day") % 60).cast("string"), 2, "0")).cast(IntegerType())
    # ).select(
    #     "time_id",
    #     "time_actual",
    #     "hours_24",
    #     "hours_12",
    #     "hour_minutes",
    #     "day_minutes",
    #     "day_time_name",
    #     "day_night"
    # )

    # # Show
    # dim_time.show()

    # # Write
    # dim_time.writeTo(f"demo.default.dim_time").overwritePartitions()
    spark.stop()

    
if __name__ == "__main__":
    """
    Main entry point for the script.
    """
    if len(sys.argv) != 3:
        sys.exit(-1)

    minio_ip = sys.argv[1]
    hive_metastore_ip = sys.argv[2]


    transform(minio_ip, hive_metastore_ip)