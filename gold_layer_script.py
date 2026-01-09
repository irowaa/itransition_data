from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# Read fact table
df_fact = spark.read.table("fact_taxi_daily")

df_fact_enriched = (
    df_fact
    .withColumn(
        "payment_type_name",
        F.when(F.col("payment_type") == 1, "Credit Card")
         .when(F.col("payment_type") == 2, "Cash")
         .when(F.col("payment_type") == 3, "No Charge")
         .when(F.col("payment_type") == 4, "Dispute")
         .when(F.col("payment_type") == 5, "Unknown")
         .when(F.col("payment_type") == 6, "Voided Trip")
         .otherwise("Other")
    )
    .withColumn(
        "payment_description",
        F.when(F.col("payment_type") == 1, "Standard credit card payment")
         .when(F.col("payment_type") == 2, "Cash payment")
         .when(F.col("payment_type") == 3, "No charge for trip")
         .when(F.col("payment_type") == 4, "Payment disputed")
         .when(F.col("payment_type") == 5, "Payment type not recorded")
         .when(F.col("payment_type") == 6, "Trip was voided")
         .otherwise("Unknown payment type")
    )
)

# Save enriched fact
df_fact_enriched.write.mode("overwrite").format("delta").saveAsTable(
    "fact_taxi_daily_enriched"
)

print(" fact_taxi_daily_enriched created successfully")

# Quick sanity check
df_fact_enriched.groupBy("payment_type", "payment_type_name") \
    .agg(
        F.sum("trip_count").alias("total_trips"),
        F.sum("total_revenue").alias("total_revenue")
    ) \
    .orderBy(F.col("total_revenue").desc()) \
    .show(truncate=False)




from pyspark.sql import functions as F

print("2. BUILDING DIMENSION TABLES (Gold Layer) - FIXED...")
print("-" * 60)


print("\na) Creating DIM_DATE...")
try:
    start_date = '2019-01-01'
    end_date = '2026-12-31'

   
    df_dates = spark.sql(f"""
        SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS calendar_date
    """)

    df_dim_date = (
        df_dates
        .withColumn("date_id", F.date_format("calendar_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("calendar_date"))
        .withColumn("month", F.month("calendar_date"))
        .withColumn("day", F.dayofmonth("calendar_date"))
        .withColumn("quarter", F.quarter("calendar_date"))
        .withColumn("day_of_week", F.dayofweek("calendar_date"))          # 1 = Sunday, 7 = Saturday
        .withColumn("week_of_year", F.weekofyear("calendar_date"))
        .withColumn("day_of_year", F.dayofyear("calendar_date"))
        .withColumn("month_name", F.date_format("calendar_date", "MMMM"))  # portable replacement for MONTHNAME
        .withColumn("day_name", F.date_format("calendar_date", "EEEE"))    # portable replacement for DAYNAME
        .withColumn("is_weekend", F.when(F.col("day_of_week").isin(1,7), F.lit(1)).otherwise(F.lit(0)))
    )

    cols_order = ["date_id", "calendar_date", "year", "month", "day", "quarter",
                  "day_of_week", "week_of_year", "day_of_year", "month_name",
                  "day_name", "is_weekend"]
    df_dim_date = df_dim_date.select(*cols_order)

    print(f"   Created {df_dim_date.count():,} date records from {start_date} to {end_date}.")
    df_dim_date.show(5, truncate=False)

    df_dim_date.write.mode("overwrite").format("delta").saveAsTable("dim_date")
    print("   DIM_DATE saved to Warehouse.\n")

except Exception as e:
    print(f"    FAILED creating DIM_DATE: {str(e)[:300]}")
print("\nb) DIM_CURRENCY already created earlier — no action needed.")


print("\nc) DIM_GDP_COUNTRY already created earlier — no action needed.")

print("\n Dimension creation step finished.")


print("\n3. BUILDING CORE FACT TABLE: FACT_TAXI_DAILY (Gold Layer)...")
print("-"*60)

try:
    # Read the clean Silver taxi data
    df_taxi_silver = spark.read.table("silver_nyc_taxi")
    print(f"   Read {df_taxi_silver.count():,} Silver taxi records.")

    df_fact_taxi_daily = df_taxi_silver.groupBy(
        F.to_date("tpep_pickup_datetime").alias("pickup_date"),
        F.col("payment_type")
    ).agg(
        F.count("*").alias("trip_count"),
        F.sum("passenger_count").alias("total_passengers"),
        F.sum("trip_distance").alias("total_distance_miles"),
        F.avg("trip_distance").alias("avg_trip_distance"),
        F.sum("fare_amount").alias("total_fare_amount"),
        F.sum("tip_amount").alias("total_tip_amount"),
        F.sum("tolls_amount").alias("total_tolls_amount"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_revenue_per_trip"),
        F.avg("trip_duration_minutes").alias("avg_trip_duration_min"),
        F.min("tpep_pickup_datetime").alias("first_trip_time"),
        F.max("tpep_pickup_datetime").alias("last_trip_time")
    ).withColumn(
        "fact_ingestion_timestamp", F.current_timestamp()
    )
    
    df_fact_taxi_daily = df_fact_taxi_daily.withColumn("taxi_fact_key", F.monotonically_increasing_id())
    

    df_fact_taxi_daily = df_fact_taxi_daily.join(
        spark.read.table("dim_date").select("date_id", "calendar_date"),
        df_fact_taxi_daily.pickup_date == F.col("calendar_date"),
        "left"
    ).drop("calendar_date")
    
    print(f"   Created {df_fact_taxi_daily.count()} daily aggregate records.")
    print("   Sample of FactTaxiDaily:")
    df_fact_taxi_daily.orderBy("pickup_date").show(10, truncate=False)
    
   
    df_fact_taxi_daily.write.mode("overwrite").format("delta").saveAsTable("fact_taxi_daily")
    print("    FACT_TAXI_DAILY saved to Warehouse.\n")
    
except Exception as e:
    print(f"    FAILED creating FACT_TAXI_DAILY: {str(e)[:400]}")

