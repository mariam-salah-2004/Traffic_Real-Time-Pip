from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Spark Session Config
spark = (
    SparkSession.builder
    .appName("TrafficStreamingLakehouse")
    .master("spark://spark-master:7077")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Kafka Raw Stream
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "traffic-topic")
    .option("startingOffsets", "latest")
    .load()
)

# Convert Binary to String
json_stream = raw_stream.selectExpr(
    "CAST(value AS STRING) as raw_json",
    "timestamp as kafka_timestamp"
)

# Flexible Schema
traffic_schema = StructType([
    # Keys
    StructField("event_id",              StringType()),
    StructField("vehicle_id",            StringType()),
    StructField("sensor_id",             StringType()),
    StructField("trip_id",               StringType()),
    StructField("incident_id",           StringType()),
 
    # Date / time
    StructField("event_time",            StringType()),   
    StructField("event_date",            StringType()),
    StructField("event_hour",            StringType()),
 
    # Road / geography
    StructField("road_id",               StringType()),
    StructField("city_zone",             StringType()),
    StructField("direction",             StringType()),
    StructField("road_surface",          StringType()),
    StructField("lane_type",             StringType()),
    StructField("lane_number",           StringType()),
    StructField("speed_limit",           StringType()),
    StructField("geo_lat",               StringType()),
    StructField("geo_lon",               StringType()),
    StructField("nearest_intersection",  StringType()),
 
    # Vehicle (denormalised subset)
    StructField("vehicle_type",          StringType()),
    StructField("fuel_type",             StringType()),
    StructField("vehicle_make",          StringType()),
    StructField("is_commercial",         BooleanType()),
    StructField("weight_class",          StringType()),
 
    # Environment / weather
    StructField("weather",               StringType()),
    StructField("temperature_c",         StringType()),
    StructField("humidity_pct",          StringType()),
    StructField("wind_speed_kmh",        StringType()),
    StructField("visibility_m",          StringType()),
    StructField("road_temp_c",           StringType()),
    StructField("is_daylight",           BooleanType()),
 
    # Traffic signal
    StructField("signal_phase",          StringType()),
    StructField("signal_time_elapsed_s", StringType()),
 
    # Incident
    StructField("incident_type",         StringType()),
    StructField("incident_severity",     StringType()),
 
    # Measures – speed & flow
    StructField("speed_kmh",             StringType()),   # StringType to catch "FAST"
    StructField("speed_variance",        StringType()),
    StructField("is_speeding",           BooleanType()),
    StructField("congestion_level",      StringType()),
    StructField("occupancy_rate",        StringType()),
    StructField("headway_secs",          StringType()),
    StructField("flow_rate_per_hour",    StringType()),
    StructField("queue_length_m",        StringType()),
    StructField("travel_time_index",     StringType()),
 
    # Measures – safety
    StructField("brake_event",           BooleanType()),
    StructField("horn_event",            BooleanType()),
    StructField("near_miss_event",       BooleanType()),
    StructField("safety_score",          StringType()),
 
    # Measures – environment
    StructField("co2_grams_per_km",      StringType()),
    StructField("noise_db",              StringType()),
    StructField("fuel_consumption_l100km", StringType()),
 
    # Measures – toll / revenue
    StructField("toll_amount_usd",       StringType()),
    StructField("payment_method",        StringType()),
    StructField("toll_tag_present",      BooleanType()),
 
    # Metadata
    StructField("data_source",           StringType()),
    StructField("schema_version",        StringType()),
    StructField("ingested_at",           StringType()),
 
    # Corrupt-event catch-all
    StructField("raw",                   StringType()),
])

parsed = json_stream.withColumn(
    "data",
    from_json(col("raw_json"), traffic_schema)
)

flattened = parsed.select(
    "raw_json",
    "kafka_timestamp",
    "data.*"
)

# Bronze Delta Write
bronze_query = (
    flattened.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/opt/spark/warehouse/chk/traffic_bronze")
    .option("path", "/opt/spark/warehouse/traffic_bronze")
    .start()
)

spark.streams.awaitAnyTermination()