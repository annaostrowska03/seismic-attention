"""Check that NiFi loaded every local Bluesky file into HDFS and every line into Kafka.

Usage (from the repo root, stack running, after the NiFi flow has processed all files):
    python scripts/verify_bsky_ingest.py             # HDFS and Kafka
    python scripts/verify_bsky_ingest.py --no-kafka  # HDFS only (reading Kafka takes a few minutes)

Checks (test cases BSKY-I-02 and BSKY-I-03 in docs/bluesky.md):
    HDFS   the same daily files, with the same sizes, as data/raw/bsky, in date=YYYY-MM-DD folders
    Kafka  committed messages in topic "bsky" = lines in the local files; every message is valid JSON
"""
import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw" / "bsky"
HDFS_DIR = "/raw/bsky"
TOPIC = "bsky"
FILE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}\.jsonl$")
COMPOSE_EXEC = ["docker", "compose", "exec", "-T"]


def local_files():
    """{file name: size in bytes} of the daily files that NiFi picks up."""
    return {p.name: p.stat().st_size for p in RAW_DIR.iterdir() if FILE_RE.match(p.name)}


def local_line_count(names):
    total = 0
    for name in names:
        with open(RAW_DIR / name, "rb") as f:
            total += sum(1 for line in f if line.strip())
    return total


def hdfs_files():
    """{file name: (size in bytes, full HDFS path)} of the files under /raw/bsky."""
    out = subprocess.run(COMPOSE_EXEC + ["namenode", "hdfs", "dfs", "-ls", "-R", HDFS_DIR],
                         capture_output=True, text=True, cwd=ROOT, check=True).stdout
    files = {}
    for line in out.splitlines():
        parts = line.split(None, 7)
        if len(parts) == 8 and parts[0].startswith("-"):
            files[parts[7].rsplit("/", 1)[-1]] = (int(parts[4]), parts[7])
    return files


def kafka_messages():
    """Read every committed message of the topic once; return (messages, invalid JSON)."""
    cmd = COMPOSE_EXEC + ["kafka", "/opt/kafka/bin/kafka-console-consumer.sh",
                          "--bootstrap-server", "kafka:9092", "--topic", TOPIC,
                          "--from-beginning", "--isolation-level", "read_committed",
                          "--timeout-ms", "30000"]
    count = invalid = 0
    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, cwd=ROOT) as p:
        for line in p.stdout:
            count += 1
            try:
                json.loads(line)
            except ValueError:
                invalid += 1
    return count, invalid


def report(name, ok, detail, examples=()):
    print(f"[{'OK' if ok else 'FAIL':>4}] {name:<32} {detail}")
    if examples:
        print("       e.g. " + ", ".join(examples[:5]))
    return ok


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--no-kafka", action="store_true", help="skip the Kafka check")
    args = ap.parse_args()

    local = local_files()
    hdfs = hdfs_files()
    missing = sorted(set(local) - set(hdfs))
    extra = sorted(set(hdfs) - set(local))
    wrong_size = sorted(n for n in set(local) & set(hdfs) if local[n] != hdfs[n][0])
    misplaced = sorted(n for n, (_, path) in hdfs.items()
                       if path != f"{HDFS_DIR}/date={n[:10]}/{n}")

    ok = True
    ok &= report("HDFS files = local files", not missing and not extra,
                 f"local={len(local)} hdfs={len(hdfs)} missing={len(missing)} extra={len(extra)}",
                 missing or extra)
    ok &= report("HDFS sizes = local sizes", not wrong_size,
                 f"bytes local={sum(local.values())} hdfs={sum(s for s, _ in hdfs.values())}"
                 f" different={len(wrong_size)}", wrong_size)
    ok &= report("HDFS layout date=YYYY-MM-DD", not misplaced, f"misplaced={len(misplaced)}",
                 misplaced)

    if not args.no_kafka:
        print("reading Kafka topic (a few minutes) ...")
        lines = local_line_count(local)
        messages, invalid = kafka_messages()
        ok &= report("Kafka messages = local lines", messages == lines,
                     f"kafka={messages} local={lines}")
        ok &= report("Kafka messages are valid JSON", invalid == 0, f"invalid={invalid}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
