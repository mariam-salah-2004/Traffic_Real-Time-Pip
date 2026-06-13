from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# =============================================================================
#  Spark Session
# =============================================================================

spark = (
    SparkSession.builder
    .appName("TrafficSilverGoldLayer")
    .master("spark://spark-master:7077")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

BASE = "/opt/spark/warehouse"

# =============================================================================
#  SILVER — Business-rule constants
# =============================================================================

VALID_VEHICLE_TYPES  = ["CAR", "TRUCK", "MOTORCYCLE", "BUS", "BICYCLE", "SCOOTER", "EMERGENCY"]
VALID_FUEL_TYPES     = ["PETROL", "DIESEL", "ELECTRIC", "HYBRID", "HYDROGEN"]
VALID_WEATHER        = ["CLEAR", "RAIN", "FOG", "STORM", "SNOW", "HAIL", "WINDY"]
VALID_ROAD_SURFACES  = ["ASPHALT", "CONCRETE", "GRAVEL", "WET_ASPHALT"]
VALID_LANE_TYPES     = ["MAIN", "EXPRESS", "BUS_LANE", "BIKE_LANE", "MERGE"]
VALID_DIRECTIONS     = ["NORTH", "SOUTH", "EAST", "WEST"]
VALID_SIGNAL_PHASES  = ["GREEN", "YELLOW", "RED", "FLASHING"]
VALID_PAYMENT        = ["CASH", "TAG", "APP", "EXEMPT"]
VALID_INCIDENT_TYPES = ["NONE", "ACCIDENT", "BREAKDOWN", "ROADWORK", "POLICE_CHECK", "DEBRIS"]
VALID_WEIGHT_CLASS   = ["LIGHT", "MEDIUM", "HEAVY"]
VALID_ZONES          = ["CBD", "AIRPORT", "TECHPARK", "SUBURB", "TRAINSTATION", "HARBOR", "UNIVERSITY"]

SPEED_MIN        = 0
SPEED_MAX        = 250
SPEED_LIMIT_VALS = [40, 50, 60, 80, 100, 110]
CONGESTION_MIN   = 1
CONGESTION_MAX   = 5
GEO_LAT_MIN, GEO_LAT_MAX   = -90.0,  90.0
GEO_LON_MIN, GEO_LON_MAX   = -180.0, 180.0
HUMIDITY_MIN, HUMIDITY_MAX  = 0, 100
NOISE_MIN,    NOISE_MAX     = 0.0, 200.0
CO2_MIN,      CO2_MAX       = 0.0, 1000.0
TOLL_MIN,     TOLL_MAX      = 0.0, 500.0

SILVER_COLUMNS = [
    "event_id", "vehicle_id", "sensor_id", "trip_id",
    "event_time_ts", "event_year", "event_month", "event_day",
    "event_hour_clean", "event_dow", "is_peak_hour",
    "ingested_at_ts", "silver_processed_at",
    "road_id", "city_zone", "direction", "road_surface",
    "lane_type", "lane_number", "speed_limit",
    "geo_lat", "geo_lon", "nearest_intersection",
    "vehicle_type", "fuel_type", "vehicle_make",
    "is_commercial", "weight_class",
    "weather", "weather_severity",
    "temperature_c", "humidity_pct", "wind_speed_kmh",
    "visibility_m", "road_temp_c", "is_daylight",
    "signal_phase", "signal_time_elapsed_s",
    "incident_type", "incident_id", "incident_severity",
    "speed_kmh", "speed_band", "speed_excess_kmh", "speed_variance",
    "is_speeding", "congestion_level",
    "occupancy_rate", "headway_secs",
    "flow_rate_per_hour", "queue_length_m", "travel_time_index",
    "brake_event", "horn_event", "near_miss_event",
    "safety_score_clean",
    "co2_grams_per_km", "noise_db", "fuel_consumption_l100km",
    "toll_amount_usd", "payment_method", "toll_tag_present",
]

# =============================================================================
#  SILVER — Transform function (تشتغل على كل batch)
# =============================================================================

def transform_to_silver(raw_df):
    is_corrupt     = col("raw").isNotNull()
    mandatory_nulls = (
        col("event_id").isNull() | col("event_time").isNull()
        | col("vehicle_id").isNull() | col("sensor_id").isNull()
    )

    quarantine = raw_df.filter(is_corrupt | mandatory_nulls).withColumn(
        "quarantine_reason",
        when(col("raw").isNotNull(),       lit("CORRUPT_JSON"))
        .when(col("event_id").isNull(),    lit("NULL_EVENT_ID"))
        .when(col("event_time").isNull(),  lit("NULL_EVENT_TIME"))
        .when(col("vehicle_id").isNull(),  lit("NULL_VEHICLE_ID"))
        .otherwise(lit("NULL_SENSOR_ID"))
    ).withColumn("quarantined_at", current_timestamp())

    valid = raw_df.filter(~is_corrupt & ~mandatory_nulls)

    # Cast
    typed = (
        valid
        .withColumn("event_time_ts",   to_timestamp(col("event_time")))
        .withColumn("ingested_at_ts",  to_timestamp(col("ingested_at")))
        .withColumn("speed_kmh",       col("speed_kmh").cast(DoubleType()))
        .withColumn("congestion_level",col("congestion_level").cast(IntegerType()))
        .withColumn("occupancy_rate",  col("occupancy_rate").cast(DoubleType()))
        .withColumn("headway_secs",    col("headway_secs").cast(DoubleType()))
        .withColumn("flow_rate_per_hour", col("flow_rate_per_hour").cast(IntegerType()))
        .withColumn("co2_grams_per_km",col("co2_grams_per_km").cast(DoubleType()))
        .withColumn("noise_db",        col("noise_db").cast(DoubleType()))
        .withColumn("toll_amount_usd", col("toll_amount_usd").cast(DoubleType()))
        .withColumn("fuel_consumption_l100km", col("fuel_consumption_l100km").cast(DoubleType()))
        .withColumn("geo_lat",         col("geo_lat").cast(DoubleType()))
        .withColumn("geo_lon",         col("geo_lon").cast(DoubleType()))
        .withColumn("temperature_c",   col("temperature_c").cast(DoubleType()))
        .withColumn("road_temp_c",     col("road_temp_c").cast(DoubleType()))
        .withColumn("humidity_pct",    col("humidity_pct").cast(IntegerType()))
        .withColumn("wind_speed_kmh",  col("wind_speed_kmh").cast(IntegerType()))
        .withColumn("visibility_m",    col("visibility_m").cast(IntegerType()))
        .withColumn("speed_limit",     col("speed_limit").cast(IntegerType()))
        .withColumn("lane_number",     col("lane_number").cast(IntegerType()))
        .withColumn("signal_time_elapsed_s", col("signal_time_elapsed_s").cast(IntegerType()))
        .withColumn("queue_length_m",  col("queue_length_m").cast(IntegerType()))
        .withColumn("travel_time_index", col("travel_time_index").cast(DoubleType()))
        .withColumn("speed_variance",  col("speed_variance").cast(DoubleType()))
        .withColumn("safety_score",    col("safety_score").cast(DoubleType()))
        .withColumn("is_speeding",     col("is_speeding").cast(BooleanType()))
        .withColumn("brake_event",     col("brake_event").cast(BooleanType()))
        .withColumn("horn_event",      col("horn_event").cast(BooleanType()))
        .withColumn("near_miss_event", col("near_miss_event").cast(BooleanType()))
        .withColumn("is_commercial",   col("is_commercial").cast(BooleanType()))
        .withColumn("toll_tag_present",col("toll_tag_present").cast(BooleanType()))
        .withColumn("is_daylight",     col("is_daylight").cast(BooleanType()))
    )

    # Standardise strings
    str_cols = [
        "vehicle_type", "fuel_type", "vehicle_make", "weight_class",
        "weather", "road_surface", "lane_type", "direction",
        "signal_phase", "incident_type", "incident_severity",
        "payment_method", "city_zone", "road_id",
    ]
    std = typed
    for c in str_cols:
        std = std.withColumn(c, when(col(c).isNotNull(), upper(trim(col(c)))).otherwise(lit(None)))

    # Clip + validate
    def clip(df, c, lo, hi):
        return df.withColumn(c,
            when(col(c).isNull(), lit(None))
            .when(col(c) < lo, lit(lo))
            .when(col(c) > hi, lit(hi))
            .otherwise(col(c)))

    def null_if_not_in(df, c, lst):
        return df.withColumn(c, when(col(c).isin(lst), col(c)).otherwise(lit(None)))

    v = std
    v = clip(v, "speed_kmh",        SPEED_MIN,    SPEED_MAX)
    v = clip(v, "congestion_level", CONGESTION_MIN, CONGESTION_MAX)
    v = clip(v, "geo_lat",          GEO_LAT_MIN,  GEO_LAT_MAX)
    v = clip(v, "geo_lon",          GEO_LON_MIN,  GEO_LON_MAX)
    v = clip(v, "humidity_pct",     HUMIDITY_MIN, HUMIDITY_MAX)
    v = clip(v, "noise_db",         NOISE_MIN,    NOISE_MAX)
    v = clip(v, "co2_grams_per_km", CO2_MIN,      CO2_MAX)
    v = clip(v, "toll_amount_usd",  TOLL_MIN,     TOLL_MAX)
    v = null_if_not_in(v, "vehicle_type",   VALID_VEHICLE_TYPES)
    v = null_if_not_in(v, "fuel_type",      VALID_FUEL_TYPES)
    v = null_if_not_in(v, "weather",        VALID_WEATHER)
    v = null_if_not_in(v, "road_surface",   VALID_ROAD_SURFACES)
    v = null_if_not_in(v, "lane_type",      VALID_LANE_TYPES)
    v = null_if_not_in(v, "direction",      VALID_DIRECTIONS)
    v = null_if_not_in(v, "signal_phase",   VALID_SIGNAL_PHASES)
    v = null_if_not_in(v, "payment_method", VALID_PAYMENT)
    v = null_if_not_in(v, "incident_type",  VALID_INCIDENT_TYPES)
    v = null_if_not_in(v, "weight_class",   VALID_WEIGHT_CLASS)
    v = null_if_not_in(v, "city_zone",      VALID_ZONES)
    v = null_if_not_in(v, "speed_limit",    [str(x) for x in SPEED_LIMIT_VALS] + SPEED_LIMIT_VALS)

    # Enrich
    enriched = (
        v
        .withColumn("speed_band",
            when(col("speed_kmh").isNull(), lit("UNKNOWN"))
            .when(col("speed_kmh") < 20,   lit("VERY_SLOW"))
            .when(col("speed_kmh") < 50,   lit("SLOW"))
            .when(col("speed_kmh") < 80,   lit("MODERATE"))
            .when(col("speed_kmh") < 110,  lit("FAST"))
            .otherwise(lit("VERY_FAST")))
        .withColumn("speed_excess_kmh",
            when(col("speed_kmh").isNotNull() & col("speed_limit").isNotNull(),
                round(greatest(col("speed_kmh") - col("speed_limit"), lit(0.0)), 2)
            ).otherwise(lit(None)))
        .withColumn("safety_score_clean",
            round(10.0
                - when(col("near_miss_event") == True, lit(3.0)).otherwise(lit(0.0))
                - when(col("brake_event")     == True, lit(1.0)).otherwise(lit(0.0)), 1))
        .withColumn("event_year",       year(col("event_time_ts")))
        .withColumn("event_month",      month(col("event_time_ts")))
        .withColumn("event_day",        dayofmonth(col("event_time_ts")))
        .withColumn("event_hour_clean", hour(col("event_time_ts")))
        .withColumn("event_dow",        dayofweek(col("event_time_ts")))
        .withColumn("is_peak_hour",     col("event_hour_clean").isin([7, 8, 17, 18]))
        .withColumn("weather_severity",
            when(col("weather").isin(["CLEAR", "WINDY"]),         lit("LOW"))
            .when(col("weather").isin(["RAIN", "FOG"]),            lit("MEDIUM"))
            .when(col("weather").isin(["STORM", "SNOW", "HAIL"]), lit("HIGH"))
            .otherwise(lit("UNKNOWN")))
        .withColumn("silver_processed_at", current_timestamp())
    )

    return enriched.select(*SILVER_COLUMNS), quarantine

# =============================================================================
#  GOLD — Surrogate key helper
# =============================================================================

def surrogate(*cols_):
    safe = [coalesce(c, lit("UNKNOWN")) for c in cols_]
    return md5(concat_ws("|", *safe))

# =============================================================================
#  GOLD — Dimension & Fact builders
# =============================================================================

def build_dim_vehicle(df):
    return df.select(
        surrogate(col("vehicle_id")).alias("vehicle_key"),
        col("vehicle_id"), col("vehicle_type"), col("fuel_type"),
        col("vehicle_make"), col("weight_class"), col("is_commercial"),
    ).dropDuplicates(["vehicle_key"])

def build_dim_road(df):
    return df.select(
        surrogate(col("road_id"), col("lane_type"), col("direction")).alias("road_key"),
        col("road_id"), col("lane_type"), col("lane_number"), col("direction"),
        col("road_surface"), col("city_zone"), col("speed_limit"),
        col("geo_lat"), col("geo_lon"), col("nearest_intersection"),
    ).dropDuplicates(["road_key"])

def build_dim_weather(df):
    return df.select(
        surrogate(col("weather"), col("weather_severity")).alias("weather_key"),
        col("weather"), col("weather_severity"), col("temperature_c"),
        col("road_temp_c"), col("humidity_pct"), col("wind_speed_kmh"),
        col("visibility_m"), col("is_daylight"),
    ).dropDuplicates(["weather_key"])

def build_dim_time(df):
    return df.select(
        surrogate(
            col("event_year").cast("string"), col("event_month").cast("string"),
            col("event_day").cast("string"),  col("event_hour_clean").cast("string"),
        ).alias("time_key"),
        col("event_time_ts"), col("event_year"), col("event_month"),
        col("event_day"), col("event_hour_clean"), col("event_dow"), col("is_peak_hour"),
    ).dropDuplicates(["time_key"])

def build_dim_signal(df):
    return df.select(
        surrogate(col("signal_phase"), col("signal_time_elapsed_s").cast("string")).alias("signal_key"),
        col("signal_phase"), col("signal_time_elapsed_s"), col("queue_length_m"),
    ).dropDuplicates(["signal_key"])

def build_dim_payment(df):
    return df.select(
        surrogate(col("payment_method")).alias("payment_key"),
        col("payment_method")
    ).dropDuplicates(["payment_key"])

def build_dim_incidentdet(df):
    return df.select(
        col("incident_id").alias("incident_key"),
        col("incident_severity"), col("incident_type"), col("speed_band")
    ).dropDuplicates(["incident_key"])

def build_fact_traffic_event(df):
    return df.select(
        col("event_id"), col("sensor_id"), col("trip_id"),
        surrogate(col("vehicle_id")).alias("vehicle_key"),
        surrogate(col("road_id"), col("lane_type"), col("direction")).alias("road_key"),
        surrogate(
            col("event_year").cast("string"), col("event_month").cast("string"),
            col("event_day").cast("string"),  col("event_hour_clean").cast("string"),
        ).alias("time_key"),
        surrogate(col("weather"), col("weather_severity")).alias("weather_key"),
        surrogate(col("signal_phase"), col("signal_time_elapsed_s").cast("string")).alias("signal_key"),
        surrogate(col("payment_method")).alias("payment_key"),
        col("speed_kmh"), col("speed_excess_kmh"), col("speed_variance"),
        col("is_speeding").cast("int").alias("is_speeding"),
        col("congestion_level"), col("occupancy_rate"), col("headway_secs"),
        col("flow_rate_per_hour"), col("travel_time_index"),
        col("horn_event").cast("int").alias("horn_event"),
        col("near_miss_event").cast("int").alias("near_miss_event"),
        col("brake_event").cast("int").alias("brake_event"),
        col("safety_score_clean"), col("co2_grams_per_km"),
        col("noise_db"), col("fuel_consumption_l100km"),
        col("toll_amount_usd"),
        col("toll_tag_present").cast("int").alias("toll_tag_present")
    )

def build_fact_incident(df):
    return (
        df.filter(col("incident_type").isNotNull() & (col("incident_type") != lit("NONE")))
        .select(
            col("incident_id").alias("incident_key"), col("event_id"),
            surrogate(col("vehicle_id")).alias("vehicle_key"),
            surrogate(col("road_id"), col("lane_type"), col("direction")).alias("road_key"),
            surrogate(
                col("event_year").cast("string"), col("event_month").cast("string"),
                col("event_day").cast("string"),  col("event_hour_clean").cast("string"),
            ).alias("time_key"),
            surrogate(col("weather"), col("weather_severity")).alias("weather_key"),
            col("speed_kmh"), col("congestion_level"), col("visibility_m"),
            col("is_peak_hour").cast("int").alias("is_peak_hour"),
            col("near_miss_event").cast("int").alias("near_miss_event"),
            col("brake_event").cast("int").alias("brake_event")
        )
    )

# =============================================================================
#  foreachBatch — Silver + Gold في نفس الـ batch
# =============================================================================

def process_batch(bronze_batch_df, batch_id):
    if bronze_batch_df.isEmpty():
        print(f"[Batch {batch_id}] Empty — skipping.")
        return

    print(f"[Batch {batch_id}] ── Silver transform...")

    silver_df, quarantine_df = transform_to_silver(bronze_batch_df)

    # كتابة Silver
    (silver_df.write.format("delta")
        .mode("append").option("mergeSchema", "true")
        .save(f"{BASE}/traffic_silver"))

    # كتابة Quarantine
    if not quarantine_df.isEmpty():
        (quarantine_df.write.format("delta")
            .mode("append").option("mergeSchema", "true")
            .save(f"{BASE}/traffic_quarantine"))

    print(f"[Batch {batch_id}] ── Gold transform...")

    # Dimensions
    (build_dim_vehicle(silver_df).write.format("delta")
        .mode("append").option("mergeSchema", "true").save(f"{BASE}/traffic_gold/dim_vehicle"))
    (build_dim_road(silver_df).write.format("delta")
        .mode("append").option("mergeSchema", "true").save(f"{BASE}/traffic_gold/dim_road"))
    (build_dim_weather(silver_df).write.format("delta")
        .mode("append").option("mergeSchema", "true").save(f"{BASE}/traffic_gold/dim_weather"))
    (build_dim_time(silver_df).write.format("delta")
        .mode("append").option("mergeSchema", "true").save(f"{BASE}/traffic_gold/dim_time"))
    (build_dim_signal(silver_df).write.format("delta")
        .mode("append").option("mergeSchema", "true").save(f"{BASE}/traffic_gold/dim_signal"))
    (build_dim_payment(silver_df).write.format("delta")
        .mode("append").option("mergeSchema", "true").save(f"{BASE}/traffic_gold/dim_payment"))
    (build_dim_incidentdet(silver_df).write.format("delta")
        .mode("append").option("mergeSchema", "true").save(f"{BASE}/traffic_gold/dim_incidentdet"))

    # Facts
    (build_fact_traffic_event(silver_df).write.format("delta")
        .mode("append").save(f"{BASE}/traffic_gold/fact_traffic_event"))
    (build_fact_incident(silver_df).write.format("delta")
        .mode("append").save(f"{BASE}/traffic_gold/fact_incident"))

    print(f"[Batch {batch_id}] ✅ Silver + Gold done.")

# =============================================================================
#  STREAM: Bronze → foreachBatch(Silver + Gold)
# =============================================================================

bronze_stream = (
    spark.readStream
    .format("delta")
    .load(f"{BASE}/traffic_bronze")
)

query = (
    bronze_stream.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", f"{BASE}/chk/traffic_silver_gold")
    .trigger(processingTime="30 seconds")
    .start()
)

print("✅ Silver+Gold streaming started. Listening to Bronze...")
query.awaitTermination()
