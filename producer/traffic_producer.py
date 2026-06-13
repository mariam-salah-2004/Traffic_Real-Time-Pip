from kafka import KafkaProducer
from faker import Faker
import json
import random
import time
import uuid
from datetime import datetime, timedelta
import pytz

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ── Dimension value pools ────────────────────────────────────────────────────

ROADS = ["R100", "R200", "R300", "R400", "R500", "R600"]
ZONES = ["CBD", "AIRPORT", "TECHPARK", "SUBURB", "TRAINSTATION", "HARBOR", "UNIVERSITY"]
WEATHER = ["CLEAR", "RAIN", "FOG", "STORM", "SNOW", "HAIL", "WINDY"]
VEHICLE_TYPES = ["CAR", "TRUCK", "MOTORCYCLE", "BUS", "BICYCLE", "SCOOTER", "EMERGENCY"]
FUEL_TYPES = ["PETROL", "DIESEL", "ELECTRIC", "HYBRID", "HYDROGEN"]
MAKES = ["Toyota", "Ford", "BMW", "Tesla", "Mercedes", "Honda", "Volvo", "Audi"]
INCIDENT_TYPES = ["NONE", "ACCIDENT", "BREAKDOWN", "ROADWORK", "POLICE_CHECK", "DEBRIS"]
ROAD_SURFACE = ["ASPHALT", "CONCRETE", "GRAVEL", "WET_ASPHALT"]
LANE_TYPES = ["MAIN", "EXPRESS", "BUS_LANE", "BIKE_LANE", "MERGE"]
DIRECTIONS = ["NORTH", "SOUTH", "EAST", "WEST"]
SIGNAL_PHASES = ["GREEN", "YELLOW", "RED", "FLASHING"]
PAYMENT_METHODS = ["CASH", "TAG", "APP", "EXEMPT"]

# ── Stateful caches (simulate FK relationships) ──────────────────────────────

vehicle_registry: dict[str, dict] = {}   # vehicle_id → dim_vehicle fields
sensor_registry: dict[str, dict] = {}    # sensor_id  → dim_sensor fields
trip_registry: dict[str, dict] = {}      # trip_id    → dim_trip fields

seen_event_ids: list[str] = []           # for duplicate injection

def _register_vehicle() -> dict:
    vid = str(uuid.uuid4())
    plate = fake.license_plate()
    entry = {
        "vehicle_id": vid,
        "license_plate": plate,
        "vehicle_type": random.choice(VEHICLE_TYPES),
        "fuel_type": random.choice(FUEL_TYPES),
        "make": random.choice(MAKES),
        "model": fake.word().capitalize(),
        "model_year": random.randint(2010, 2024),
        "registered_city": fake.city(),
        "is_commercial": random.random() < 0.3,
        "passenger_capacity": random.randint(1, 60),
        "weight_class": random.choice(["LIGHT", "MEDIUM", "HEAVY"]),
    }
    vehicle_registry[vid] = entry
    return entry


def _register_sensor(road_id: str) -> dict:
    sid = f"SEN-{road_id}-{random.randint(1, 20):03d}"
    if sid in sensor_registry:
        return sensor_registry[sid]
    entry = {
        "sensor_id": sid,
        "sensor_type": random.choice(["LOOP", "RADAR", "CAMERA", "LIDAR", "BLUETOOTH"]),
        "road_id": road_id,
        "sensor_lat": round(random.uniform(-33.9, -33.8), 6),
        "sensor_lon": round(random.uniform(151.1, 151.3), 6),
        "lane_number": random.randint(1, 4),
        "lane_type": random.choice(LANE_TYPES),
        "speed_limit": random.choice([40, 50, 60, 80, 100, 110]),
        "calibrated_at": (datetime.now(pytz.utc) - timedelta(days=random.randint(1, 365))).isoformat(),
    }
    sensor_registry[sid] = entry
    return entry


def _register_trip(vehicle_id: str) -> dict:
    tid = str(uuid.uuid4())
    entry = {
        "trip_id": tid,
        "vehicle_id": vehicle_id,
        "origin_zone": random.choice(ZONES),
        "destination_zone": random.choice(ZONES),
        "trip_start_time": (datetime.now(pytz.utc) - timedelta(minutes=random.randint(1, 120))).isoformat(),
        "estimated_duration_mins": random.randint(5, 90),
        "trip_purpose": random.choice(["COMMUTE", "DELIVERY", "LEISURE", "EMERGENCY", "SCHOOL"]),
        "toll_tag_id": fake.uuid4() if random.random() < 0.6 else None,
        "payment_method": random.choice(PAYMENT_METHODS),
    }
    trip_registry[tid] = entry
    return entry


# ── Fact event builder ───────────────────────────────────────────────────────

def generate_clean_event() -> dict:
    # Resolve or create dimension members
    if vehicle_registry and random.random() < 0.7:
        vehicle = random.choice(list(vehicle_registry.values()))
    else:
        vehicle = _register_vehicle()

    road_id = random.choice(ROADS)
    sensor = _register_sensor(road_id)

    if trip_registry and random.random() < 0.4:
        trip = random.choice(list(trip_registry.values()))
    else:
        trip = _register_trip(vehicle["vehicle_id"])

    event_time = datetime.now(pytz.utc)
    event_id = str(uuid.uuid4())
    seen_event_ids.append(event_id)

    # ── Fact columns ─────────────────────────────────────────────────────────
    speed = random.randint(20, 100)
    speed_limit = sensor["speed_limit"]
    congestion_level = random.randint(1, 5)
    occupancy_rate = round(random.uniform(0.0, 1.0), 3)
    headway_secs = round(random.uniform(1.0, 20.0), 2)
    flow_rate_per_hour = int(3600 / max(headway_secs, 1))
    toll_amount = round(random.uniform(0.0, 8.50), 2)
    co2_grams_per_km = round(random.uniform(0, 300), 1)
    noise_db = round(random.uniform(45.0, 95.0), 1)
    visibility_m = random.choice([50, 100, 200, 500, 1000, 5000])
    road_temp_c = round(random.uniform(-5.0, 55.0), 1)
    brake_event = random.random() < 0.15
    horn_event = random.random() < 0.05
    near_miss = random.random() < 0.02
    has_incident = random.random() < 0.15
    return {
        # ── Surrogate / PK ────────────────────────────────────────────────
        "event_id": event_id,

        # ── FK references to dimension tables ─────────────────────────────
        "vehicle_id":        vehicle["vehicle_id"],
        "sensor_id":         sensor["sensor_id"],
        "trip_id":           trip["trip_id"],

        # ── Dim: Date / Time (for DATE_DIM join) ──────────────────────────
        "event_time":        event_time.isoformat(),
        "event_date":        event_time.strftime("%Y-%m-%d"),
        "event_hour":        event_time.hour,

        # ── Dim: Road / Geography ─────────────────────────────────────────
        "road_id":           road_id,
        "city_zone":         random.choice(ZONES),
        "direction":         random.choice(DIRECTIONS),
        "road_surface":      random.choice(ROAD_SURFACE),
        "lane_type":         sensor["lane_type"],
        "lane_number":       sensor["lane_number"],
        "speed_limit":       speed_limit,
        "geo_lat":           sensor["sensor_lat"] + round(random.uniform(-0.001, 0.001), 6),
        "geo_lon":           sensor["sensor_lon"] + round(random.uniform(-0.001, 0.001), 6),

        # ── Dim: Vehicle (denormalized subset) ───────────────────────────
        "vehicle_type":      vehicle["vehicle_type"],
        "fuel_type":         vehicle["fuel_type"],
        "vehicle_make":      vehicle["make"],
        "is_commercial":     vehicle["is_commercial"],
        "weight_class":      vehicle["weight_class"],

        # ── Dim: Environment / Weather ────────────────────────────────────
        "weather":           random.choice(WEATHER),
        "temperature_c":     round(random.uniform(-10.0, 45.0), 1),
        "humidity_pct":      random.randint(10, 100),
        "wind_speed_kmh":    random.randint(0, 90),
        "visibility_m":      visibility_m,
        "road_temp_c":       road_temp_c,
        "is_daylight":       6 <= event_time.hour <= 20,

        # ── Dim: Traffic Signal ───────────────────────────────────────────
        "signal_phase":         random.choice(SIGNAL_PHASES),
        "signal_time_elapsed_s": random.randint(0, 120),
        "nearest_intersection":  f"INT-{road_id}-{random.randint(1, 50):02d}",

        # ── Dim: Incident ─────────────────────────────────────────────────
        "incident_type":     random.choice([t for t in INCIDENT_TYPES if t != "NONE"]) if has_incident else "NONE",
        "incident_id":       str(uuid.uuid4()) if has_incident else None,
        "incident_severity": random.choice(["LOW", "MEDIUM", "HIGH"]) if has_incident else None,
        # ── Measures: Speed & Flow ────────────────────────────────────────
        "speed_kmh":            speed,
        "speed_variance":       round(random.uniform(0, 15), 2),
        "is_speeding":          speed > speed_limit,
        "congestion_level":     congestion_level,
        "occupancy_rate":       occupancy_rate,
        "headway_secs":         headway_secs,
        "flow_rate_per_hour":   flow_rate_per_hour,
        "queue_length_m":       random.randint(0, 500) if congestion_level >= 4 else 0,
        "travel_time_index":    round(1 + (congestion_level - 1) * 0.3, 3),

        # ── Measures: Safety ──────────────────────────────────────────────
        "brake_event":       brake_event,
        "horn_event":        horn_event,
        "near_miss_event":   near_miss,
        "safety_score":      round(10 - (3 * int(near_miss)) - (1 * int(brake_event)), 1),

        # ── Measures: Environment ─────────────────────────────────────────
        "co2_grams_per_km":  co2_grams_per_km,
        "noise_db":          noise_db,
        "fuel_consumption_l100km": round(random.uniform(4.0, 25.0), 2),

        # ── Measures: Toll / Revenue ──────────────────────────────────────
        "toll_amount_usd":   toll_amount,
        "payment_method":    trip["payment_method"],
        "toll_tag_present":  trip["toll_tag_id"] is not None,

        # ── Metadata ──────────────────────────────────────────────────────
        "data_source":       "SENSOR_FEED_V2",
        "schema_version":    "2.1.0",
        "ingested_at":       datetime.now(pytz.utc).isoformat(),
    }


# ── Dirty event injector ─────────────────────────────────────────────────────

DIRTY_TYPES = [
    "null_speed", "negative_speed", "extreme_speed",
    "duplicate_event", "late_event", "future_event",
    "wrong_datatype", "schema_drift", "corrupt_json",
    "null_vehicle_id", "invalid_geo", "missing_sensor",
    "congestion_out_of_range", "null_event_time",
]


def generate_dirty_event():
    dirty_type = random.choice(DIRTY_TYPES)
    base = generate_clean_event()

    if dirty_type == "null_speed":
        base["speed_kmh"] = None

    elif dirty_type == "negative_speed":
        base["speed_kmh"] = -40

    elif dirty_type == "extreme_speed":
        base["speed_kmh"] = random.choice([300, 420, 999, 1500])

    elif dirty_type == "duplicate_event" and seen_event_ids:
        base["event_id"] = random.choice(seen_event_ids)

    elif dirty_type == "late_event":
        base["event_time"] = (
            datetime.now(pytz.utc) - timedelta(minutes=random.randint(10, 180))
        ).isoformat()

    elif dirty_type == "future_event":
        base["event_time"] = (
            datetime.now(pytz.utc) + timedelta(minutes=random.randint(5, 120))
        ).isoformat()

    elif dirty_type == "wrong_datatype":
        base["speed_kmh"] = "FAST"
        base["congestion_level"] = "HIGH"

    elif dirty_type == "schema_drift":
        # Simulate a producer sending a new / unexpected field
        base["road_condition"] = random.choice(["GOOD", "BAD", "UNDER_CONSTRUCTION"])
        base["driver_mood"] = random.choice(["CALM", "AGGRESSIVE", "DISTRACTED"])
        del base["schema_version"]

    elif dirty_type == "corrupt_json":
        return "###CORRUPTED_EVENT###"

    elif dirty_type == "null_vehicle_id":
        base["vehicle_id"] = None
        base["vehicle_type"] = None

    elif dirty_type == "invalid_geo":
        base["geo_lat"] = 9999.9
        base["geo_lon"] = -9999.9

    elif dirty_type == "missing_sensor":
        base["sensor_id"] = None
        base["lane_number"] = None

    elif dirty_type == "congestion_out_of_range":
        base["congestion_level"] = random.choice([-1, 0, 6, 10, 99])

    elif dirty_type == "null_event_time":
        base["event_time"] = None
        base["event_date"] = None
        base["event_hour"] = None

    return base


# ── Main loop ────────────────────────────────────────────────────────────────

while True:
    if random.random() < 0.8:
        event = generate_clean_event()
        tag = "CLEAN"
    else:
        event = generate_dirty_event()
        tag = "DIRTY"

    if isinstance(event, str):
        producer.send("traffic-topic", value={"raw": event, "schema_version": "2.1.0"})
        print(f"[CORRUPT] raw string sent")
    else:
        producer.send("traffic-topic", value=event)

    time.sleep(random.uniform(1, 2))


