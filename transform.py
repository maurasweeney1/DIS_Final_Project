# transform.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("WeatherEnergyPipeline").getOrCreate()
PROJECT = "cpsc482-final"
RAW = f"gs://{PROJECT}-raw"
PROCESSED = f"gs://{PROJECT}-processed"

# Load raw data
weather = spark.read.csv(f"{RAW}/weather/*.csv", header=True, inferSchema=True)
eia = spark.read.csv(f"{RAW}/eia/*.csv", header=True, inferSchema=True)

# Standardize timestamps 
weather = weather.withColumn("timestamp", F.to_timestamp("timestamp"))
eia = eia.withColumn("timestamp", F.to_timestamp("period"))  # EIA uses 'period' column

#  Aggregate weather to BA region level 
# Multiple cities to one BA region, population weighted average
city_weights = spark.createDataFrame([
    # (city_name, balancing_authority, population_weight)
    ("seattle", "BPAT", 0.6), ("portland", "BPAT", 0.4),
    # all 50 city to BA mappings
], ["city", "balancing_authority", "weight"])

weather_with_weights = weather.join(city_weights, on=["city", "balancing_authority"])

weather_agg = (weather_with_weights
    .groupBy("balancing_authority", "timestamp")
    .agg(
        F.sum(F.col("temperature_2m") * F.col("weight")).alias("temp_weighted"),
        F.avg("relative_humidity_2m").alias("humidity_avg"),
        F.avg("wind_speed_10m").alias("wind_avg"),
        F.avg("shortwave_radiation").alias("solar_avg"),
        F.avg("cloud_cover").alias("cloud_avg"),
        F.sum("precipitation").alias("precip_total")
    )
)

#  Null cleaning: seasonal median 
# For EIA data with 2-5% nulls in small BAs
w = Window.partitionBy("respondent", F.month("timestamp"), F.hour("timestamp"))
eia_clean = (eia
    .withColumn("value_filled",
        F.when(F.col("value").isNull(), F.percentile_approx("value", 0.5).over(w))
         .otherwise(F.col("value")))
)

# Pivot to get demand, forecast, generation as separate columns
eia_pivoted = (eia_clean
    .filter(F.col("type-name").isin(["Demand", "Day-ahead demand forecast", "Net generation"]))
    .groupBy("respondent", "period")
    .pivot("type-name")
    .agg(F.first("value_filled"))
    .withColumnRenamed("Demand", "demand_mwh")
    .withColumnRenamed("Day-ahead demand forecast", "forecast_mwh")
    .withColumnRenamed("Net generation", "net_gen_mwh")
    .withColumn("timestamp", F.to_timestamp("period"))
)

# Join weather, energy 
fact = weather_agg.join(
    eia_pivoted,
    on=[weather_agg.balancing_authority == eia_pivoted.respondent,
        weather_agg.timestamp == eia_pivoted.timestamp],
    how="inner"
).drop(eia_pivoted.timestamp).drop(eia_pivoted.respondent)

# Feature engineering 
fact = (fact
    .withColumn("hour_of_day", F.hour("timestamp"))
    .withColumn("day_of_week", F.dayofweek("timestamp"))
    .withColumn("month", F.month("timestamp"))
    .withColumn("year", F.year("timestamp"))
    .withColumn("is_weekend", (F.dayofweek("timestamp").isin([1,7])).cast("int"))
    # Heating/cooling degree hours (base 65°F = 18.3°C)
    .withColumn("hdh", F.greatest(F.lit(18.3) - F.col("temp_weighted"), F.lit(0.0)))
    .withColumn("cdh", F.greatest(F.col("temp_weighted") - F.lit(18.3), F.lit(0.0)))
    # Split into train/eval/test
    # Train up to 2021, eval in 2022, rest test
    .withColumn("split",
        F.when(F.col("year") <= 2021, "train")
         .when(F.col("year") == 2022, "eval")
         .otherwise("test"))
)

#  Write to GCS (Parquet, partitioned) 
(fact
    .repartition("balancing_authority", "year")
    .write
    .partitionBy("balancing_authority", "year")
    .mode("overwrite")
    .parquet(f"{PROCESSED}/fact_table/")
)

print(f"Fact table rows: {fact.count()}")
spark.stop()