import pytest

from seismic_attention.features import SPARK_SCHEMA, local_hour, parse_place, usgs_features


@pytest.mark.parametrize("place, expected", [
    ("72 km SSE of Ancud, Chile", (72.0, "Ancud", "Chile")),
    ("215 km NW of Port-Olry, Vanuatu", (215.0, "Port-Olry", "Vanuatu")),
    ("4 km N of The Geysers, CA", (4.0, "The Geysers", "CA")),
    ("12 km N of Somewhere", (12.0, "Somewhere", None)),
    ("Izu Islands, Japan region", (None, None, "Japan")),
    ("Kermadec Islands, New Zealand", (None, None, "New Zealand")),
    ("Fiji region", (None, None, "Fiji")),
    ("south of the Fiji Islands", (None, None, "south of the Fiji Islands")),
    ("Mid-Indian Ridge", (None, None, "Mid-Indian Ridge")),
    ("", (None, None, None)),
    (None, (None, None, None)),
])
def test_parse_place(place, expected):
    assert parse_place(place) == expected


def test_local_hour():
    noon_utc = 1788264000000 # 2026-09-01 12:00:00 UTC
    assert local_hour(noon_utc, 0) == 12
    assert local_hour(noon_utc, 139.7) == 21  # Tokyo
    assert local_hour(noon_utc, -70.6) == 7  # Santiago
    assert local_hour(noon_utc, 180) == 0
    assert local_hour(noon_utc, -180) == 0


def test_usgs_features():
    # M5.0 near Ancud, 2026-11-11 19:37:20 UTC (Wednesday)
    features = usgs_features(5, 35, -42.5, -73.6, 1794425840000, "72 km SSE of Ancud, Chile")
    assert features == {
        "mag": 5.0,
        "depth_km": 35.0,
        "latitude": -42.5,
        "longitude": -73.6,
        "hour_utc": 19,
        "hour_local": 14,
        "weekday": 2,
        "place_distance_km": 72.0,
        "place_name": "Ancud",
        "place_region": "Chile",
    }


def test_spark_schema_matches_feature_names():
    features = usgs_features(5, 35, -42.5, -73.6, 1794425840000, None)
    schema_names = [column.split()[0] for column in SPARK_SCHEMA.split(", ")]
    assert schema_names == list(features)
