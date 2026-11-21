"""Tests of the regular expressions used by the NiFi GDELT flow (scripts/nifi_gdelt_flow.py, processor ExtractText).

NiFi (Java) and Python regular expressions agree for these patterns. ExtractText takes the first match
of capture group 1, which is re.search(...).group(1) in Python. No NiFi needed.
"""
import re

import nifi_gdelt_flow as flow

EXTRACT = flow.PROCESSORS["extract_url"]["properties"]

# format of https://data.gdeltproject.org/gdeltv2/lastupdate.txt: "size md5 url", one line per file type
LASTUPDATE = (
    "120335 4e07a6a3e4f5d0c1c0e7d6a8e5a5b0f1 http://data.gdeltproject.org/gdeltv2/20261121170000.export.CSV.zip\n"
    "198342 9f8d6c5b4a3e2d1c0b9a8f7e6d5c4b3a http://data.gdeltproject.org/gdeltv2/20261121170000.mentions.CSV.zip\n"
    "3102948 0a1b2c3d4e5f60718293a4b5c6d7e8f9 http://data.gdeltproject.org/gdeltv2/20261121170000.gkg.csv.zip\n"
)


def extract(name, text):
    m = re.search(EXTRACT[name], text)
    return m.group(1) if m else None


def test_gkg_url_is_the_gkg_line_only():
    assert extract("gkg_url", LASTUPDATE) == "http://data.gdeltproject.org/gdeltv2/20261121170000.gkg.csv.zip"


def test_gdelt_stamp_is_the_time_of_the_gkg_file():
    assert extract("gdelt_stamp", LASTUPDATE) == "20261121170000"


def test_https_url_is_accepted():
    line = "1 x https://data.gdeltproject.org/gdeltv2/20250327100000.gkg.csv.zip\n"
    assert extract("gkg_url", line) == "https://data.gdeltproject.org/gdeltv2/20250327100000.gkg.csv.zip"


def test_no_gkg_line_gives_no_match():
    # ExtractText sends the file to "unmatched" (auto-terminated): nothing is downloaded or logged
    text = LASTUPDATE.splitlines()[0] + "\n" + LASTUPDATE.splitlines()[1] + "\n"
    assert extract("gkg_url", text) is None
    assert extract("gdelt_stamp", text) is None


def test_translated_file_has_no_stamp():
    # translated files (20261121170000.translation.gkg.csv.zip) are listed in lastupdate-translation.txt,
    # which the flow does not read; gkg_url alone would match such a line, gdelt_stamp does not
    line = "1 x http://data.gdeltproject.org/gdeltv2/20261121170000.translation.gkg.csv.zip\n"
    assert extract("gdelt_stamp", line) is None


def test_hdfs_directories_follow_the_team_convention():
    assert flow.HDFS_DIR.startswith("/raw/gdelt/date=")
    assert flow.HDFS_STATUS_DIR.startswith("/raw/gdelt_files/date=")
