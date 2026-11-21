"""Create, run and export the NiFi flow that ingests Bluesky files (NiFi 1.28 REST API).

Flow (process group "bluesky-ingest"):

    ListFile (/opt/app/data/raw/bsky, YYYY-MM-DD.jsonl)
      -> FetchFile
           -> PutHDFS        /raw/bsky/date=YYYY-MM-DD/YYYY-MM-DD.jsonl   (raw zone, whole file)
           -> PublishKafka   topic "bsky", one message per line            (stream / replay path)

Usage (from the repo root, stack running):
    python scripts/nifi_bsky_flow.py create     # build the process group (fails if it exists)
    python scripts/nifi_bsky_flow.py start      # start all processors
    python scripts/nifi_bsky_flow.py status     # processor states and queued flow files
    python scripts/nifi_bsky_flow.py stop
    python scripts/nifi_bsky_flow.py export     # save the flow definition to nifi/flows/bsky.json
    python scripts/nifi_bsky_flow.py delete     # stop, empty queues and remove the group

The exported JSON can also be imported in the NiFi UI: drag a Process Group onto the canvas
and use the upload button next to the name field.
"""
import argparse
import json
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
API = "http://localhost:8080/nifi-api"
GROUP_NAME = "bluesky-ingest"
EXPORT_FILE = ROOT / "nifi" / "flows" / "bsky.json"

SOURCE_DIR = "/opt/app/data/raw/bsky"
HADOOP_CONF = "/opt/nifi/hadoop-conf/core-site.xml,/opt/nifi/hadoop-conf/hdfs-site.xml"
HDFS_DIR = "/raw/bsky/date=${filename:substringBefore('.jsonl')}"
KAFKA_BROKERS = "kafka:9092"
KAFKA_TOPIC = "bsky"

STD = "org.apache.nifi.processors.standard."
PROCESSORS = {
    "list": {
        "type": STD + "ListFile",
        "name": "List raw Bluesky files",
        "position": {"x": 0, "y": 0},
        "schedulingPeriod": "60 sec",
        "properties": {
            "Input Directory": SOURCE_DIR,
            "File Filter": r"\d{4}-\d{2}-\d{2}\.jsonl",   # skips _done.txt
            "Recurse Subdirectories": "false",
        },
        "autoTerminate": [],
    },
    "fetch": {
        "type": STD + "FetchFile",
        "name": "Fetch file content",
        "position": {"x": 0, "y": 250},
        "properties": {"Completion Strategy": "None"},   # never touch the source files
        "autoTerminate": ["not.found", "permission.denied", "failure"],
    },
    "hdfs": {
        "type": "org.apache.nifi.processors.hadoop.PutHDFS",
        "name": "Put file to HDFS raw zone",
        "position": {"x": -350, "y": 500},
        "properties": {
            "Hadoop Configuration Resources": HADOOP_CONF,
            "Directory": HDFS_DIR,
            "Conflict Resolution Strategy": "replace",   # re-runs are idempotent
        },
        "autoTerminate": ["success"],
    },
    "kafka": {
        "type": "org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6",
        "name": "Publish lines to Kafka bsky",
        "position": {"x": 350, "y": 500},
        "properties": {
            "bootstrap.servers": KAFKA_BROKERS,
            "topic": KAFKA_TOPIC,
            # one Kafka message per JSONL line; a trailing \r (Windows files) is JSON whitespace
            "message-demarcator": "\n",
        },
        "autoTerminate": ["success"],
    },
}
CONNECTIONS = [
    ("list", "fetch", ["success"]),
    ("fetch", "hdfs", ["success"]),
    ("fetch", "kafka", ["success"]),
    ("hdfs", "hdfs", ["failure"]),     # retry until HDFS accepts the file
    ("kafka", "kafka", ["failure"]),   # retry until Kafka accepts the messages
]


def call(method, path, **kwargs):
    r = requests.request(method, API + path, timeout=60, **kwargs)
    if r.status_code >= 400:
        raise SystemExit(f"{method} {path} -> HTTP {r.status_code}: {r.text[:300]}")
    return r.json() if r.content and "json" in r.headers.get("content-type", "") else r.text


def root_id():
    return call("GET", "/flow/process-groups/root")["processGroupFlow"]["id"]


def find_group():
    groups = call("GET", f"/flow/process-groups/{root_id()}")["processGroupFlow"]["flow"]["processGroups"]
    return next((g for g in groups if g["component"]["name"] == GROUP_NAME), None)


def require_group():
    group = find_group()
    if group is None:
        raise SystemExit(f"process group '{GROUP_NAME}' not found - run 'create' first")
    return group


def create():
    if find_group():
        raise SystemExit(f"process group '{GROUP_NAME}' already exists - run 'delete' first")
    group = call("POST", f"/process-groups/{root_id()}/process-groups", json={
        "revision": {"version": 0},
        "component": {"name": GROUP_NAME, "position": {"x": 0, "y": 0},
                      "comments": "Bluesky JSONL files -> HDFS raw zone + Kafka topic bsky"},
    })
    gid = group["id"]
    ids = {}
    for key, spec in PROCESSORS.items():
        config = {"properties": spec["properties"], "autoTerminatedRelationships": spec["autoTerminate"]}
        if "schedulingPeriod" in spec:
            config["schedulingPeriod"] = spec["schedulingPeriod"]
        proc = call("POST", f"/process-groups/{gid}/processors", json={
            "revision": {"version": 0},
            "component": {"type": spec["type"], "name": spec["name"], "position": spec["position"],
                          "config": config},
        })
        ids[key] = proc["id"]
        print(f"processor {spec['name']}: {proc['component']['validationStatus']}"
              f" {proc['component'].get('validationErrors') or ''}")
    for src, dst, rels in CONNECTIONS:
        call("POST", f"/process-groups/{gid}/connections", json={
            "revision": {"version": 0},
            "component": {
                "source": {"id": ids[src], "groupId": gid, "type": "PROCESSOR"},
                "destination": {"id": ids[dst], "groupId": gid, "type": "PROCESSOR"},
                "selectedRelationships": rels,
            },
        })
    print(f"created process group {GROUP_NAME} ({gid})")


def set_state(state):
    gid = require_group()["id"]
    call("PUT", f"/flow/process-groups/{gid}", json={"id": gid, "state": state})
    print(f"{GROUP_NAME}: {state}")


def status():
    gid = require_group()["id"]
    flow = call("GET", f"/flow/process-groups/{gid}")["processGroupFlow"]["flow"]
    for p in flow["processors"]:
        snap = p["status"]["aggregateSnapshot"]
        print(f"{p['component']['name']:<30} {p['component']['state']:<8} "
              f"in={snap['flowFilesIn']} out={snap['flowFilesOut']} (last 5 min)")
    for c in flow["connections"]:
        snap = c["status"]["aggregateSnapshot"]
        print(f"queue {c['component']['source']['name']} -> {c['component']['destination']['name']}"
              f" [{','.join(c['component']['selectedRelationships'])}]: {snap['queued']}")
    for b in call("GET", f"/flow/bulletin-board?groupId={gid}&limit=10")["bulletinBoard"]["bulletins"]:
        print("BULLETIN:", b.get("bulletin", {}).get("message", "")[:200])


def export():
    gid = require_group()["id"]
    flow = call("GET", f"/process-groups/{gid}/download")
    EXPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    data = flow if isinstance(flow, dict) else json.loads(flow)
    EXPORT_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"exported to {EXPORT_FILE.relative_to(ROOT)}")


def delete():
    group = require_group()
    gid = group["id"]
    call("PUT", f"/flow/process-groups/{gid}", json={"id": gid, "state": "STOPPED"})
    for c in call("GET", f"/flow/process-groups/{gid}")["processGroupFlow"]["flow"]["connections"]:
        call("POST", f"/flowfile-queues/{c['id']}/drop-requests")
    time.sleep(2)   # drop requests run asynchronously; NiFi refuses to delete non-empty queues
    version = call("GET", f"/process-groups/{gid}")["revision"]["version"]
    call("DELETE", f"/process-groups/{gid}?version={version}")
    print(f"deleted {GROUP_NAME}")


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    commands = {"create": create, "start": lambda: set_state("RUNNING"),
                "stop": lambda: set_state("STOPPED"), "status": status,
                "export": export, "delete": delete}
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=commands)
    commands[ap.parse_args().command]()


if __name__ == "__main__":
    main()
