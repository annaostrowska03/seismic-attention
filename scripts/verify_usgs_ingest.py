# Checks of what the NiFi USGS flow wrote to HDFS and Kafka. Run from the repo root: python scripts/verify_usgs_ingest.py
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone

KAFKA_DAYS = 6  # Kafka keeps messages for 7 days, older ones are only in HDFS


def run(*cmd):
    out = subprocess.run(["docker", "compose", "exec", "-T", *cmd], capture_output=True, check=True)
    return out.stdout.decode("utf-8")


def parse(text):
    # files without a newline at the end are glued together by `cat`, so not split by lines
    decoder, pos, records = json.JSONDecoder(), 0, []
    text = text.strip()
    while pos < len(text):
        record, pos = decoder.raw_decode(text, pos)
        records.append(record)
        while pos < len(text) and text[pos].isspace():
            pos += 1
    return records


def version(record):
    return record["id"], record["properties"]["updated"]


listing = run("namenode", "hdfs", "dfs", "-ls", "-R", "/raw/usgs")
paths = [line.split()[-1] for line in listing.splitlines() if line.startswith("-")]
hdfs = parse(run("namenode", "hdfs", "dfs", "-cat", "/raw/usgs/date=*/*"))
kafka = parse(run("kafka", "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "kafka:9092",
                  "--topic", "usgs", "--from-beginning", "--isolation-level", "read_committed",
                  "--timeout-ms", "20000"))

# file usgs-yyyyMMddHHmmss-... must be in the folder date=yyyy-MM-dd
misplaced = 0
for path in paths:
    folder, name = path.split("/")[-2:]
    day = name[5:13]
    misplaced += folder != f"date={day[:4]}-{day[4:6]}-{day[6:]}"

incomplete = [r for r in hdfs if not (r.get("id") and r.get("properties", {}).get("updated")
                                      and r.get("ingest_time_utc"))]
hdfs_versions = [version(r) for r in hdfs if r not in incomplete]
kafka_versions = {version(r) for r in kafka}
since = (datetime.now(timezone.utc) - timedelta(days=KAFKA_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
recent = {version(r) for r in hdfs if r not in incomplete and r["ingest_time_utc"] >= since}

# (check, result, expected)
checks = [
    (f"files not in date= folder of their day (of {len(paths)})", misplaced, 0),
    (f"records without id, updated or ingest_time_utc (of {len(hdfs)})", len(incomplete), 0),
    ("event versions stored twice", len(hdfs_versions) - len(set(hdfs_versions)), 0),
    (f"Kafka messages not in HDFS (of {len(kafka_versions)})", len(kafka_versions - set(hdfs_versions)), 0),
    (f"HDFS records of last {KAFKA_DAYS} days not in Kafka (of {len(recent)})", len(recent - kafka_versions), 0),
]

failed = 0
for name, result, expected in checks:
    ok = result == expected
    failed += not ok
    print(f"{'OK  ' if ok else 'FAIL'} {name}: {result} (expected {expected})")
sys.exit(1 if failed else 0)
