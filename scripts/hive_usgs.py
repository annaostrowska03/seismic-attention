"""Create the USGS Hive table and check the raw events written by NiFi.

Usage (from the repo root, stack running):
    python scripts/hive_usgs.py create    # run hive/usgs.sql (external table + flat view, safe to re-run)
    python scripts/hive_usgs.py verify    # sanity checks of usgs_raw

New days written by NiFi become visible after "create" (it runs MSCK REPAIR TABLE).
SQL goes to beeline through stdin, so nothing has to be copied into the container.
"""
import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SQL_FILE = ROOT / "hive" / "usgs.sql"
BEELINE = ["docker", "compose", "exec", "-T", "hiveserver2", "beeline",
           "-u", "jdbc:hive2://localhost:10000/", "--silent=true", "--showHeader=false",
           "--outputformat=csv2", "-f", "/dev/stdin"]

# name, query, expected value (None = only printed)
CHECKS = [
    ("rows", "SELECT count(*) FROM seismic.usgs_raw", None),
    ("events", "SELECT count(DISTINCT id) FROM seismic.usgs_raw", None),
    # NiFi drops versions it has already seen, so (id, updated) must be unique
    ("repeated_versions",
     "SELECT count(*) FROM (SELECT id, properties.updated FROM seismic.usgs_raw "
     "GROUP BY id, properties.updated HAVING count(*) > 1) t", 0),
    ("missing_id_time_mag",
     "SELECT count(*) FROM seismic.usgs_raw WHERE id IS NULL OR properties.`time` IS NULL "
     "OR properties.mag IS NULL", 0),
    ("bad_coordinates",
     "SELECT count(*) FROM seismic.usgs_raw WHERE geometry.coordinates IS NULL "
     "OR size(geometry.coordinates) != 3", 0),
    ("lat_lon_out_of_range",
     "SELECT count(*) FROM seismic.usgs_raw_flat WHERE abs(latitude) > 90 OR abs(longitude) > 180", 0),
    ("partition_date_mismatch",
     "SELECT count(*) FROM seismic.usgs_raw WHERE `date` != substr(ingest_time_utc, 1, 10)", 0),
]


def beeline(sql: str) -> str:
    r = subprocess.run(BEELINE, input=sql.encode("utf-8"), capture_output=True, cwd=ROOT)
    out = r.stdout.decode("utf-8", "replace")
    if r.returncode != 0:
        sys.stderr.write(r.stderr.decode("utf-8", "replace")[-2000:])
        raise SystemExit(f"beeline failed with exit code {r.returncode}")
    return out


def create() -> None:
    beeline(SQL_FILE.read_text(encoding="utf-8"))
    print(beeline("SHOW PARTITIONS seismic.usgs_raw;").strip())


def verify() -> int:
    failed = 0
    for name, sql, expected in CHECKS:
        value = int(beeline(sql + ";").strip().splitlines()[-1])
        if expected is None:
            print(f"[INFO] {name:<24} {value}")
            continue
        ok = value == expected
        failed += not ok
        print(f"[{'OK' if ok else 'FAIL':>4}] {name:<24} hive={value} expected={expected}")
    print(beeline("SELECT `date`, count(*), min(mag), max(mag) FROM seismic.usgs_raw_flat "
                  "GROUP BY `date` ORDER BY `date`;").strip())
    return 1 if failed else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["create", "verify"])
    if ap.parse_args().command == "create":
        create()
        return 0
    return verify()


if __name__ == "__main__":
    sys.exit(main())
