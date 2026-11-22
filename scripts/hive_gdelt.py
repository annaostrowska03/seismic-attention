"""Create the GDELT Hive tables and check that the articles agree with the file log.

Usage (from the repo root, stack running):
    python scripts/hive_gdelt.py create    # run hive/gdelt.sql (external tables, safe to re-run)
    python scripts/hive_gdelt.py verify    # gdelt_raw against gdelt_files_raw (both written by NiFi)

New days written by NiFi become visible after "create" (it runs MSCK REPAIR TABLE).
SQL goes to beeline through stdin, so nothing has to be copied into the container.
"""
import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SQL_FILE = ROOT / "hive" / "gdelt.sql"
BEELINE = ["docker", "compose", "exec", "-T", "hiveserver2", "beeline",
           "-u", "jdbc:hive2://localhost:10000/", "--silent=true", "--showHeader=false",
           "--outputformat=csv2", "-f", "/dev/stdin"]

# name, query, expected value (None = the value of "log_articles")
CHECKS = [
    ("log_articles", "SELECT coalesce(sum(articles), 0) FROM seismic.gdelt_files_raw WHERE status = 'ok'", None),
    ("raw_articles", "SELECT count(*) FROM seismic.gdelt_raw", None),
    ("raw_unique_ids", "SELECT count(DISTINCT gkg_id) FROM seismic.gdelt_raw", None),
    # every file with articles in the log has a data file, and the other way round
    ("files_only_in_log",
     "SELECT count(*) FROM (SELECT `file` FROM seismic.gdelt_files_raw WHERE articles > 0) l "
     "LEFT JOIN (SELECT DISTINCT substr(gkg_id, 1, 14) AS f FROM seismic.gdelt_raw) r ON l.`file` = r.f "
     "WHERE r.f IS NULL", 0),
    ("files_only_in_raw",
     "SELECT count(*) FROM (SELECT DISTINCT substr(gkg_id, 1, 14) AS f FROM seismic.gdelt_raw) r "
     "LEFT JOIN (SELECT `file` FROM seismic.gdelt_files_raw) l ON l.`file` = r.f WHERE l.`file` IS NULL", 0),
    ("unknown_status",
     "SELECT count(*) FROM seismic.gdelt_files_raw WHERE status NOT IN ('ok', 'missing', 'broken')", 0),
    ("partition_date_mismatch",
     "SELECT count(*) FROM seismic.gdelt_files_raw WHERE `date` != concat_ws('-', substr(`file`, 1, 4), "
     "substr(`file`, 5, 2), substr(`file`, 7, 2))", 0),
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
    print(beeline("SHOW PARTITIONS seismic.gdelt_raw; SHOW PARTITIONS seismic.gdelt_files_raw;").strip())


def verify() -> int:
    values = {}
    for name, sql, _ in CHECKS:
        # map-join runs as a local task that fails in this container; a normal join is fine for this size
        out = beeline("SET hive.auto.convert.join=false;\n" + sql + ";")
        values[name] = int(out.strip().splitlines()[-1])
    failed = 0
    for name, _, expected in CHECKS:
        if name == "log_articles":
            print(f"[INFO] {name:<24} {values[name]}  (sum of articles in the file log)")
            continue
        exp = values["log_articles"] if expected is None else expected
        ok = values[name] == exp
        failed += not ok
        print(f"[{'OK' if ok else 'FAIL':>4}] {name:<24} hive={values[name]} expected={exp}")
    print(beeline("SELECT status, count(*), sum(articles) FROM seismic.gdelt_files_raw GROUP BY status;").strip())
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
