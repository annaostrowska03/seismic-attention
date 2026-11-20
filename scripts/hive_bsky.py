"""Create the Bluesky Hive tables and check their row counts against the Spark job summary.

Usage (from the repo root, stack running):
    python scripts/hive_bsky.py create    # run hive/bsky.sql (external tables, safe to re-run)
    python scripts/hive_bsky.py verify    # compare Hive counts with data/processed/bsky_summary.json

SQL goes to beeline through stdin, so nothing has to be copied into the container.
"""
import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SQL_FILE = ROOT / "hive" / "bsky.sql"
SUMMARY_FILE = ROOT / "data" / "processed" / "bsky_summary.json"
BEELINE = ["docker", "compose", "exec", "-T", "hiveserver2", "beeline",
           "-u", "jdbc:hive2://localhost:10000/", "--silent=true", "--showHeader=false",
           "--outputformat=csv2", "-f", "/dev/stdin"]

VERIFY_SQL = """
SELECT 'raw_rows', count(*) FROM seismic.bsky_raw
UNION ALL SELECT 'raw_files', count(DISTINCT `date`) FROM seismic.bsky_raw
UNION ALL SELECT 'unique_posts', count(*) FROM seismic.bsky_posts
UNION ALL SELECT 'unique_uri_in_posts', count(DISTINCT uri) FROM seismic.bsky_posts
UNION ALL SELECT 'in_period', count(*) FROM seismic.bsky_posts WHERE in_period
UNION ALL SELECT 'clean', count(*) FROM seismic.bsky_posts_clean
UNION ALL SELECT 'minute_rows', count(*) FROM seismic.bsky_minute
UNION ALL SELECT 'minute_posts', sum(n_posts) FROM seismic.bsky_minute;
"""


def beeline(sql: str) -> str:
    r = subprocess.run(BEELINE, input=sql.encode("utf-8"), capture_output=True, cwd=ROOT)
    out = r.stdout.decode("utf-8", "replace")
    if r.returncode != 0:
        sys.stderr.write(r.stderr.decode("utf-8", "replace")[-2000:])
        raise SystemExit(f"beeline failed with exit code {r.returncode}")
    return out


def create() -> None:
    beeline(SQL_FILE.read_text(encoding="utf-8"))
    print(beeline("USE seismic; SHOW TABLES;").strip())


def verify() -> int:
    summary = json.loads(SUMMARY_FILE.read_text(encoding="utf-8"))
    start, end = (datetime.fromisoformat(d) for d in summary["period"])
    expected = {
        "raw_rows": summary["raw_rows"],
        "raw_files": summary["raw_files"],
        "unique_posts": summary["unique_posts"],
        "unique_uri_in_posts": summary["unique_posts"],
        "in_period": summary["in_period"],
        "clean": summary["clean"],
        "minute_rows": int((end - start).total_seconds() // 60),
        "minute_posts": summary["in_period"],
    }
    actual = {}
    for line in beeline(VERIFY_SQL).splitlines():
        parts = line.strip().split(",")
        if len(parts) == 2 and parts[0] in expected:
            actual[parts[0]] = int(parts[1])
    failed = 0
    for key, exp in expected.items():
        got = actual.get(key)
        ok = got == exp
        failed += not ok
        print(f"[{'OK' if ok else 'FAIL':>4}] {key:<20} hive={got} expected={exp}")
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
