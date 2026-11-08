"""Checks that every service of the docker-compose stack works. Run from the repo root:

    python scripts/smoke_test.py            # all checks
    python scripts/smoke_test.py kafka hdfs # selected checks
"""
import subprocess
import sys
import urllib.request

COMPOSE = ["docker", "compose", "exec", "-T"]


def run(*args: str) -> str:
    result = subprocess.run([*COMPOSE, *args], capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        raise RuntimeError((result.stdout + result.stderr).strip()[-800:])
    return result.stdout


def check_postgres() -> None:
    run("postgres", "psql", "-U", "seismic", "-d", "seismic", "-c", "SELECT 1")
    run("postgres", "psql", "-U", "hive", "-d", "metastore", "-c", "SELECT 1")


def check_kafka() -> None:
    topics = run("kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092", "--list").split()
    missing = {"usgs", "bsky", "gdelt"} - set(topics)
    if missing:
        raise RuntimeError(f"missing topics: {missing}")


def check_hdfs() -> None:
    listing = run("namenode", "hdfs", "dfs", "-ls", "/raw")
    for source in ("usgs", "bsky", "gdelt"):
        if f"/raw/{source}" not in listing:
            raise RuntimeError(f"/raw/{source} missing in HDFS")


def check_hive() -> None:
    run("hiveserver2", "beeline", "-u", "jdbc:hive2://localhost:10000/", "-e", "SHOW DATABASES")


def check_spark() -> None:
    out = run("spark-master", "/opt/spark/bin/spark-submit", "/opt/app/scripts/spark_smoke_job.py")
    if "SMOKE OK" not in out:
        raise RuntimeError(out[-800:])


def check_nifi() -> None:
    with urllib.request.urlopen("http://localhost:8080/nifi-api/flow/about", timeout=10) as response:
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}")


CHECKS = {
    "postgres": check_postgres,
    "kafka": check_kafka,
    "hdfs": check_hdfs,
    "hive": check_hive,
    "spark": check_spark,
    "nifi": check_nifi,
}


def main() -> int:
    selected = sys.argv[1:] or list(CHECKS)
    failed = 0
    for name in selected:
        try:
            CHECKS[name]()
            print(f"[ OK ] {name}")
        except Exception as exc:  # noqa: BLE001 - report every failing service
            failed += 1
            print(f"[FAIL] {name}: {exc}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
