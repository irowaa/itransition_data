from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, DoubleType, DateType
from datetime import datetime

silver_taxi_path = "Files/silver/nyc_taxi"
silver_gdp_path = "Files/silver/worldbank_gdp"
silver_fx_path = "Files/silver/ecb_fx"


try:
    # Read from Bronze Delta table
    df_taxi_bronze = spark.read.table("bronze_nyc_taxi_yellow")
    print(f"   Read {df_taxi_bronze.count():,} raw records.")

   
    df_taxi_silver = df_taxi_bronze.filter(
        # Remove records with invalid coordinates (NYC bounding box approx.)
        (F.col("pickup_longitude").between(-74.05, -73.75)) &
        (F.col("pickup_latitude").between(40.58, 40.90)) &
        (F.col("dropoff_longitude").between(-74.05, -73.75)) &
        (F.col("dropoff_latitude").between(40.58, 40.90)) &
        # Remove invalid trip times/amounts
        (F.col("tpep_pickup_datetime") < F.col("tpep_dropoff_datetime")) &
        (F.col("total_amount") > 0) &
        (F.col("passenger_count") > 0)
    ).withColumn(
        # Calculate trip duration in minutes
        "trip_duration_minutes",
        F.round((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60, 2)
    ).withColumn(
        # Extract date and hour for time-based analysis
        "pickup_date", F.to_date("tpep_pickup_datetime")
    ).withColumn(
        "pickup_hour", F.hour("tpep_pickup_datetime")
    ).withColumn(
        # Create a trip category based on duration
        "trip_type",
        F.when(F.col("trip_duration_minutes") < 15, "short")
         .when(F.col("trip_duration_minutes") <= 45, "medium")
         .otherwise("long")
    ).withColumn(
        # Standardize payment type description
        "payment_type_desc",
        F.when(F.col("payment_type") == 1, "Credit card")
         .when(F.col("payment_type") == 2, "Cash")
         .when(F.col("payment_type") == 3, "No charge")
         .when(F.col("payment_type") == 4, "Dispute")
         .otherwise("Unknown")
    ).withColumn(
        # Add transformation metadata
        "silver_processed_timestamp", F.current_timestamp()
    ).select(
        # Select and reorder relevant columns for Silver
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "pickup_date",
        "pickup_hour",
        "passenger_count",
        "trip_distance",
        "pickup_longitude",
        "pickup_latitude",
        "dropoff_longitude",
        "dropoff_latitude",
        "payment_type",
        "payment_type_desc",
        "fare_amount",
        "tip_amount",
        "tolls_amount",
        "total_amount",
        "trip_duration_minutes",
        "trip_type",
        "ingestion_timestamp",
        "silver_processed_timestamp"
    )

    print(f"   After cleaning: {df_taxi_silver.count():,} valid records.")
    print("   Sample of Silver data:")
    df_taxi_silver.show(5, truncate=False)

    # Save to Silver layer as a new Delta TABLE
    print("   Saving to Silver layer as 'silver_nyc_taxi'...")
    df_taxi_silver.write.mode("overwrite").format("delta").saveAsTable("silver_nyc_taxi")
    print("   NYC Taxi Silver transformation complete!\n")

except Exception as e:
    print(f"   FAILED: {str(e)[:300]}")


from pyspark.sql import functions as F

print("1. TRANSFORMING NYC TAXI DATA (Silver Layer)...")
print("-" * 50)

try:
    df_bronze = spark.read.table("bronze_nyc_taxi_yellow")

    # ---- CORE VALIDATION ----
    df_silver = df_bronze.filter(
        (F.col("tpep_pickup_datetime").isNotNull()) &
        (F.col("tpep_dropoff_datetime").isNotNull()) &
        (F.col("tpep_pickup_datetime") < F.col("tpep_dropoff_datetime")) &
        (F.col("total_amount") > 0) &
        (F.col("passenger_count") > 0) &
        (F.col("trip_distance") >= 0)
    )

    # ---- ENRICHMENTS ----
    df_silver = df_silver.withColumn(
        "trip_duration_minutes",
        F.round(
            (F.unix_timestamp("tpep_dropoff_datetime") -
             F.unix_timestamp("tpep_pickup_datetime")) / 60, 2
        )
    ).withColumn(
        "pickup_date", F.to_date("tpep_pickup_datetime")
    ).withColumn(
        "pickup_hour", F.hour("tpep_pickup_datetime")
    ).withColumn(
        "trip_type",
        F.when(F.col("trip_duration_minutes") < 15, "short")
         .when(F.col("trip_duration_minutes") <= 45, "medium")
         .otherwise("long")
    ).withColumn(
        "payment_type_desc",
        F.when(F.col("payment_type") == 1, "Credit card")
         .when(F.col("payment_type") == 2, "Cash")
         .when(F.col("payment_type") == 3, "No charge")
         .when(F.col("payment_type") == 4, "Dispute")
         .otherwise("Unknown")
    ).withColumn(
        "silver_processed_timestamp", F.current_timestamp()
    )

    # ---- FINAL COLUMN SET ----
    df_silver = df_silver.select(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "pickup_date",
        "pickup_hour",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "payment_type_desc",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
        "Airport_fee",
        "total_amount",
        "trip_duration_minutes",
        "trip_type",
        "ingestion_timestamp",
        "silver_processed_timestamp"
    )

    print(f"   Silver records: {df_silver.count():,}")
    df_silver.show(5, truncate=False)

  
    df_silver.write.mode("overwrite").format("delta").saveAsTable("silver_nyc_taxi")
    print("    NYC Taxi Silver layer created successfully")

except Exception as e:
    print(f"   FAILED: {str(e)[:300]}")

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

print("\n2. TRANSFORMING WORLD BANK GDP DATA (Silver Layer)...")
print("-" * 50)

try:
    df_gdp_bronze = spark.read.table("bronze_worldbank_gdp")
    print(f"   Read {df_gdp_bronze.count():,} GDP records.")

    # Window per country ordered by year
    win = Window.partitionBy("country_code").orderBy("year")

    df_gdp_silver = (
        df_gdp_bronze
        .withColumn("year", F.col("year").cast(IntegerType()))
        .withColumn("prev_gdp", F.lag("gdp_usd", 1).over(win))
        .withColumn(
            "yoy_growth_pct",
            F.when(
                (F.col("prev_gdp").isNull()) | (F.col("prev_gdp") == 0),
                None
            ).otherwise(
                F.round(((F.col("gdp_usd") - F.col("prev_gdp")) / F.col("prev_gdp")) * 100, 2)
            )
        )
        .withColumn(
            "gdp_usd_billions",
            F.round(F.col("gdp_usd") / 1e9, 2)
        )
        .withColumn(
            "silver_processed_timestamp",
            F.current_timestamp()
        )
        .select(
            "country_code",
            "year",
            "gdp_usd",
            "gdp_usd_billions",
            "yoy_growth_pct",
            "ingestion_timestamp",
            "silver_processed_timestamp"
        )
        .orderBy("country_code", "year")
    )

    print("   Transformed GDP data:")
    df_gdp_silver.show(truncate=False)

    print("   Saving to Silver layer as 'silver_worldbank_gdp'...")
    df_gdp_silver.write.mode("overwrite").format("delta").saveAsTable("silver_worldbank_gdp")

    print("    World Bank GDP Silver transformation complete!\n")

except Exception as e:
    print(f"    FAILED: {str(e)[:300]}")



try:
    df_fx_bronze = spark.read.table("bronze_ecb_fx")
    print(f"   Read {df_fx_bronze.count()} daily FX rates.")

    # Create a complete date series to handle weekends/holidays (no trading)
    # First, get the min and max dates in our data
    date_range = df_fx_bronze.agg(F.min("date").alias("min_date"), F.max("date").alias("max_date")).collect()[0]
    min_date, max_date = date_range["min_date"], date_range["max_date"]

    # Create a DataFrame with all dates in the range
    all_dates = spark.sql(f"SELECT explode(sequence(to_date('{min_date}'), to_date('{max_date}'), interval 1 day)) as date")
    
    # Join with original data (left join) to see missing dates
    df_fx_complete = all_dates.join(df_fx_bronze, "date", "left")

    # Forward-fill missing rates (carry last known observation forward)
    from pyspark.sql.window import Window
    window_spec = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    df_fx_silver = df_fx_complete.withColumn(
        "exchange_rate_filled",
        F.last("exchange_rate", ignorenulls=True).over(window_spec)
    ).withColumn(
        # Calculate 7-day moving average for smoothing
        "exchange_rate_7d_avg",
        F.round(F.avg("exchange_rate_filled").over(Window.orderBy("date").rowsBetween(-6, 0)), 4)
    ).withColumn(
        "silver_processed_timestamp", F.current_timestamp()
    ).select(
        "date",
        "exchange_rate",  # original rate (null on weekends)
        "exchange_rate_filled",  # forward-filled rate
        "exchange_rate_7d_avg",
        "base_currency",
        "target_currency",
        "ingestion_timestamp",
        "silver_processed_timestamp"
    ).orderBy("date")

    print(f"   Created continuous series from {min_date} to {max_date}.")
    print("   Latest 10 days with forward-filling and moving avg:")
    df_fx_silver.orderBy(F.desc("date")).show(10, truncate=False)

    # Save to Silver
    print("   Saving to Silver layer as 'silver_ecb_fx'...")
    df_fx_silver.write.mode("overwrite").format("delta").saveAsTable("silver_ecb_fx")
    print("   ECB FX Silver transformation complete!\n")

except Exception as e:
    print(f"   FAILED: {str(e)}")

print("\n Table Overview:")
tables = ["silver_nyc_taxi", "silver_worldbank_gdp", "silver_ecb_fx"]
for table in tables:
    try:
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]['cnt']
        print(f"   {table}: {count:,} rows")
    except:
        print(f"   {table}: Not found or error")

print("\n Key Quality Checks:")
print("1. NYC Taxi - Invalid records removed:")
raw_count = spark.sql("SELECT COUNT(*) FROM bronze_nyc_taxi_yellow").collect()[0][0]
silver_count = spark.sql("SELECT COUNT(*) FROM silver_nyc_taxi").collect()[0][0]
print(f"   Removed {raw_count - silver_count:,} invalid records ({((raw_count-silver_count)/raw_count*100):.1f}%)")

print("\n2. GDP Data - Growth calculation check:")
gdp_check = spark.sql("""
    SELECT year, gdp_usd_billions, yoy_growth_pct 
    FROM silver_worldbank_gdp 
    WHERE yoy_growth_pct IS NOT NULL 
    ORDER BY year DESC 
    LIMIT 3
""")
gdp_check.show(truncate=False)

print("\n3. FX Data - Missing date handling:")
fx_gaps = spark.sql("""
    SELECT 
        COUNT(*) as total_days,
        SUM(CASE WHEN exchange_rate IS NULL THEN 1 ELSE 0 END) as missing_original,
        MIN(date) as date_from,
        MAX(date) as date_to
    FROM silver_ecb_fx
""")
fx_gaps.show(truncate=False)


