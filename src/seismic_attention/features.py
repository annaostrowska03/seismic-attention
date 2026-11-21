import re
from datetime import datetime, timezone

# e.g. "72 km SSE of Ancud, Chile"
PLACE_PATTERN = re.compile(r"^(\d+) km ([NSEW]{1,3}) of (.+)$")

# return type of usgs_features() for a Spark UDF
SPARK_SCHEMA = (
    "mag double, depth_km double, latitude double, longitude double, "
    "hour_utc int, hour_local int, weekday int, "
    "place_distance_km double, place_name string, place_region string"
)


def parse_place(place):
    """Splits the USGS location text into (distance_km, name, region).
    Examples:
    "72 km SSE of Ancud, Chile" -> (72.0, "Ancud", "Chile")
    "Izu Islands, Japan region" -> (None, None, "Japan")
    "south of the Fiji Islands" -> (None, None, "south of the Fiji Islands")
    """
    if not place:
        return None, None, None
    distance, name, region = None, None, place.strip()
    match = PLACE_PATTERN.match(region)
    if match:
        distance = float(match.group(1))
        name, _, region = match.group(3).rpartition(",")
        if not name:
            name, region = region, None
        name = name.strip()
    elif "," in region:
        region = region.rsplit(",", 1)[1]
    if region:
        region = region.strip()
        if region.endswith(" region"):
            region = region[: -len(" region")]
    return distance, name, region


def to_float(value):
    # USGS sends whole numbers as ints (mag 5, depth 10); Spark needs floats in double columns
    return None if value is None else float(value)


def local_hour(time_ms, longitude):
    """Approximate local (solar) hour from UTC time and longitude, 15 degrees per hour."""
    hours = time_ms / 3_600_000 + longitude / 15
    return int(hours % 24)


def usgs_features(mag, depth_km, latitude, longitude, time_ms, place):
    """Feature dict for one event. time_ms is the USGS origin time (epoch milliseconds)."""
    origin = datetime.fromtimestamp(time_ms / 1000, tz=timezone.utc)
    distance, name, region = parse_place(place)
    return {
        "mag": to_float(mag),
        "depth_km": to_float(depth_km),
        "latitude": to_float(latitude),
        "longitude": to_float(longitude),
        "hour_utc": origin.hour,
        "hour_local": local_hour(time_ms, longitude),
        "weekday": origin.weekday(), # 0 is Monday
        "place_distance_km": distance,
        "place_name": name,
        "place_region": region,
    }
