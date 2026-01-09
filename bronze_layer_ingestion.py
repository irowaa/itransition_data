# Import libraries
from pyspark.sql import SparkSession, functions as F
import requests
import pandas as pd
from datetime import datetime, timedelta
spark = SparkSession.builder.getOrCreate()

nyc_taxi_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

try:

    print("   Reading Parquet file from URL via pandas...")
    df_taxi_pandas = pd.read_parquet(nyc_taxi_url)
    df_taxi = spark.createDataFrame(df_taxi_pandas)
    
    
    df_taxi = df_taxi.withColumn("ingestion_timestamp", F.current_timestamp())
    
    print(f"    Success! Read {df_taxi.count():,} records.")
    print("   Sample schema:")
    df_taxi.printSchema()
    df_taxi.select("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "total_amount").show(5, truncate=False)
    
    # SAVE TO BRONZE: Use saveAsTable to register in Lakehouse catalog
    print("   Saving to Lakehouse as Delta table 'bronze_nyc_taxi_yellow'...")
    df_taxi.write.mode("overwrite").format("delta").saveAsTable("bronze_nyc_taxi_yellow")
    print("   NYC Taxi data saved successfully to 'bronze_nyc_taxi_yellow'.\n")
    
except Exception as e:
    print(f"   FAILED: {str(e)[:200]}")  



# ===== CELL 3: INGEST WORLD BANK GDP DATA (JSON API) =====
print("\n2. INGESTING WORLD BANK GDP DATA (USA, 2019-2024)...")
print("-"*40)

# Configuration
COUNTRY_CODE = "USA"
INDICATOR_CODE = "NY.GDP.MKTP.CD"  # GDP (current US$)
START_YEAR = 2019
END_YEAR = 2024

try:
    # 1. Fetch from World Bank API
    api_url = f"https://api.worldbank.org/v2/country/{COUNTRY_CODE}/indicator/{INDICATOR_CODE}"
    params = {"format": "json", "date": f"{START_YEAR}:{END_YEAR}", "per_page": 10000}
    
    print(f"   Calling API: {api_url}")
    response = requests.get(api_url, params=params, timeout=30)
    data = response.json()
    
   
    records = data[1]
    print(f"   Received {len(records)} data point(s).")
    
 
    pdf = pd.DataFrame(records)
    pdf = pdf[['countryiso3code', 'date', 'value']].copy()
    pdf.rename(columns={'countryiso3code': 'country_code', 
                        'date': 'year', 
                        'value': 'gdp_usd'}, inplace=True)
    

    df_gdp = spark.createDataFrame(pdf)
    df_gdp = df_gdp.withColumn("ingestion_timestamp", F.current_timestamp()) \
                   .withColumn("year", F.col("year").cast("int")) \
                   .withColumn("gdp_usd", F.col("gdp_usd").cast("double"))
    
    print("   Data preview:")
    df_gdp.show(10, truncate=False)
    
    
    print("   Saving to Lakehouse as Delta table 'bronze_worldbank_gdp'...")
    df_gdp.write.mode("overwrite").format("delta").saveAsTable("bronze_worldbank_gdp")
    print("   World Bank GDP data saved successfully to 'bronze_worldbank_gdp'.\n")
    
except Exception as e:
    print(f"   FAILED: {str(e)[:200]}")




# ===== CELL 4: INGEST ECB FX RATES (CSV from Web) =====
print("\n3. INGESTING ECB FX DATA (USD/EUR, Daily)...")
print("-"*40)

ECB_CSV_URL = "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata"

try:
    # 1. FIXED APPROACH: Use pandas to read the CSV from URL
    print("   Downloading CSV from ECB API via pandas...")
    df_ecb_pandas = pd.read_csv(ECB_CSV_URL)
    print(f"   Raw CSV shape: {df_ecb_pandas.shape}")
    
    # 2. Convert to Spark DataFrame and select relevant columns
    df_ecb_raw = spark.createDataFrame(df_ecb_pandas)
    
    # 3. Process data: ECB CSV contains columns 'TIME_PERIOD' and 'OBS_VALUE'
    df_ecb = df_ecb_raw.select(F.col("TIME_PERIOD").alias("date"), 
                                F.col("OBS_VALUE").alias("exchange_rate"))
    
    # Convert and clean
    df_ecb = df_ecb.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd")) \
                   .withColumn("exchange_rate", F.col("exchange_rate").cast("double")) \
                   .withColumn("ingestion_timestamp", F.current_timestamp()) \
                   .withColumn("base_currency", F.lit("USD")) \
                   .withColumn("target_currency", F.lit("EUR"))
    
    # Filter for recent data (last year) and drop duplicates
    one_year_ago = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    df_ecb = df_ecb.filter(F.col("date") >= one_year_ago).dropDuplicates(["date"])
    
    print(f"   Processed {df_ecb.count()} unique daily exchange rates.")
    print("   Latest 5 rates:")
    df_ecb.orderBy(F.desc("date")).show(5, truncate=False)
    
    # 4. SAVE TO BRONZE: Use saveAsTable
    print("   Saving to Lakehouse as Delta table 'bronze_ecb_fx'...")
    df_ecb.write.mode("overwrite").format("delta").saveAsTable("bronze_ecb_fx")
    print("   ECB FX data saved successfully to 'bronze_ecb_fx'.\n")
    
except Exception as e:
    print(f"    FAILED: {str(e)}")



try:
    tables = spark.sql("SHOW TABLES LIKE 'bronze_%'").collect()
    if tables:
        print(f"   Found {len(tables)} Bronze table(s):")
        for row in tables:
            print(f"   - {row['tableName']}")
    else:
        print("   No Bronze tables found. There may have been an error.")
        
    # Optional: Quick row count for each table
    print("\n📈 Row count verification:")
    for table in ["bronze_nyc_taxi_yellow", "bronze_worldbank_gdp", "bronze_ecb_fx"]:
        try:
            count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]['cnt']
            print(f"   {table}: {count:,} rows")
        except:
            print(f"   {table}: Could not verify (table may not exist)")
            
except Exception as e:
    print(f"   Verification error: {e}")

