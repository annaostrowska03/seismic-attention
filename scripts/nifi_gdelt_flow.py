"""Create, run and export the NiFi flow that ingests GDELT GKG files (NiFi 1.28 REST API).

Flow (process group "gdelt-ingest"), live mode: every 15 minutes the newest GKG file.

    InvokeHTTP lastupdate.txt (every 15 min)
      -> ExtractText       gkg_url + gdelt_stamp (e.g. 20261121170000, UTC)
      -> DetectDuplicate   skip a file that was already taken
      -> InvokeHTTP        download the zip            404 / 4xx -> status "missing"
      -> UnpackContent                                  broken zip -> status "broken"
      -> QueryRecord       EARTHQUAKE rows as JSON lines (same rule and fields as
                           scripts/backfill_gdelt.py)    unreadable csv -> status "broken"
      -> UpdateAttribute   filename <stamp>.jsonl
           -> status "ok" with the number of articles
           -> RouteOnAttribute (at least 1 article)
                -> PutHDFS       /raw/gdelt/date=YYYY-MM-DD/<stamp>.jsonl
                -> PublishKafka  topic "gdelt", one message per article

    File log (like data/raw/gdelt_history/files/*.csv of backfill_gdelt.py), one line per file:
        {"file": "<stamp>", "status": "ok" | "missing" | "broken", "articles": N}
        -> PutHDFS /raw/gdelt_files/date=YYYY-MM-DD/<stamp>.jsonl
    Files that the flow never asked for (NiFi or the laptop switched off) have no line at all:
    gaps are found by comparing the log with the 96 expected files per day.

Usage (from the repo root, stack running):
    python scripts/nifi_gdelt_flow.py create     # build the process group (fails if it exists)
    python scripts/nifi_gdelt_flow.py start      # start all processors
    python scripts/nifi_gdelt_flow.py status     # processor states and queued flow files
    python scripts/nifi_gdelt_flow.py stop
    python scripts/nifi_gdelt_flow.py export     # save the flow definition to nifi/flows/gdelt.json
    python scripts/nifi_gdelt_flow.py delete     # stop, empty queues and remove the group
"""
import argparse
import json
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
API = "http://localhost:8080/nifi-api"
GROUP_NAME = "gdelt-ingest"
EXPORT_FILE = ROOT / "nifi" / "flows" / "gdelt.json"

LASTUPDATE_URL = "https://data.gdeltproject.org/gdeltv2/lastupdate.txt"
# same conventions as the Bluesky flow (scripts/nifi_bsky_flow.py): raw zone /raw/<source>/date=YYYY-MM-DD/
HADOOP_CONF = "/opt/nifi/hadoop-conf/core-site.xml,/opt/nifi/hadoop-conf/hdfs-site.xml"
# gdelt_stamp is "20261121164500" (time of the GDELT file, UTC): the date is its first 8 characters
DATE_PART = "date=${gdelt_stamp:substring(0,4)}-${gdelt_stamp:substring(4,6)}-${gdelt_stamp:substring(6,8)}"
HDFS_DIR = "/raw/gdelt/" + DATE_PART
HDFS_STATUS_DIR = "/raw/gdelt_files/" + DATE_PART
KAFKA_BROKERS = "kafka:9092"
KAFKA_TOPIC = "gdelt"
CACHE_PORT = "4559"   # Ania's USGS flow uses 4558 for its own cache


def status_line(status, articles):
    """ReplaceText settings that turn a flow file into one line of the file log."""
    return {
        "Replacement Strategy": "Always Replace",
        "Evaluation Mode": "Entire text",
        "Replacement Value": '{"file":"${gdelt_stamp}","status":"%s","articles":%s}' % (status, articles),
    }

STD = "org.apache.nifi.processors.standard."
PROCESSORS = {
    "lastupdate": {
        "type": STD + "InvokeHTTP",
        "name": "Fetch GDELT lastupdate.txt",
        "position": {"x": 0, "y": 0},
        "schedulingPeriod": "15 min",   # GDELT publishes a new file every 15 minutes
        "properties": {
            "HTTP Method": "GET",
            "Remote URL": LASTUPDATE_URL,
            "Follow Redirects": "True",   # this NiFi version accepts only True / False
        },
        # "Response" goes on to the next processor; the rest is dropped
        "autoTerminate": ["Original", "Retry", "No Retry", "Failure"],
    },
    "extract_url": {
        "type": STD + "ExtractText",
        "name": "Extract GKG url",
        "position": {"x": 0, "y": 200},
        "properties": {
            # lastupdate.txt has 3 lines (export, mentions, gkg): "size md5 url". Take the gkg url
            # into the attribute gkg_url (capture group 1). Only the first match is used.
            "gkg_url": r"(https?://\S+\.gkg\.csv\.zip)",
            # the time in the file name, e.g. 20261121170000: used for dedup, paths and the log
            "gdelt_stamp": r"(\d{14})\.gkg\.csv\.zip",
        },
        # no gkg line in lastupdate.txt: no file name, so nothing to log; the next run tries again
        "autoTerminate": ["unmatched"],
    },
    "dedup": {
        "type": STD + "DetectDuplicate",
        "name": "Skip files already taken",
        "position": {"x": 0, "y": 400},
        "properties": {
            "Cache Entry Identifier": "${gdelt_stamp}",
            "Age Off Duration": "7 days",
            "Distributed Cache Service": "service:cache_client",
        },
        # lastupdate.txt still shows the same file (GDELT late): drop it
        "autoTerminate": ["duplicate"],
    },
    "download": {
        "type": STD + "InvokeHTTP",
        "name": "Download GKG zip",
        "position": {"x": 0, "y": 600},
        "properties": {
            "HTTP Method": "GET",
            "Remote URL": "${gkg_url}",       # attribute set by "Extract GKG url"
            "Follow Redirects": "True",      # the url is http://, the server redirects to https://
            "Connection Timeout": "60 sec",
            "Read Timeout": "60 sec",
        },
        # Response (the zip) -> unpack. No Retry (4xx, e.g. 404) -> status "missing".
        # Retry (5xx) and Failure (network, timeout) -> try again (loop, NiFi waits 30 s between tries)
        "autoTerminate": ["Original"],
    },
    "unpack": {
        "type": STD + "UnpackContent",
        "name": "Unpack GKG zip",
        "position": {"x": 0, "y": 800},
        "properties": {"Packaging Format": "zip"},   # one entry inside: <stamp>.gkg.csv
        # success (the csv) -> filter. failure (broken zip) -> status "broken", as in Ania's script
        "autoTerminate": ["original"],
    },
    "filter": {
        "type": STD + "QueryRecord",
        "name": "Keep EARTHQUAKE rows",
        "position": {"x": 0, "y": 1000},
        "properties": {
            "record-reader": "service:gkg_reader",    # API names of "Record Reader / Writer"
            "record-writer": "service:jsonl_writer",
            # a property that is not a setting becomes an output relationship: "earthquake".
            # Same rule as scripts/backfill_gdelt.py: "EARTHQUAKE" in column 8 (themes), rows
            # with fewer than 10 columns are dropped (their themes are NULL here).
            # COALESCE: the CSV reader turns an empty field into NULL, Ania's script keeps ''
            "earthquake": "SELECT gkg_id, COALESCE(\"date\", '') AS \"date\", "
                          "COALESCE(source, '') AS source, COALESCE(url, '') AS url, "
                          "themes, COALESCE(locations, '') AS locations "
                          "FROM FLOWFILE WHERE themes LIKE '%EARTHQUAKE%'",
            # a file without earthquake articles still comes out (record.count = 0): it is logged as "ok"
            "include-zero-record-flowfiles": "true",
        },
        # "earthquake" (the JSON lines) -> "Set file name". failure (csv unreadable) -> status "broken"
        "autoTerminate": ["original"],
    },
    "name": {
        "type": "org.apache.nifi.processors.attributes.UpdateAttribute",
        "name": "Set file name",
        "position": {"x": 0, "y": 1200},
        "properties": {
            "filename": "${gdelt_stamp}.jsonl",   # e.g. 20261121164500.jsonl
        },
        "autoTerminate": [],
    },
    "has_rows": {
        "type": STD + "RouteOnAttribute",
        "name": "Only files with articles",
        "position": {"x": 0, "y": 1400},
        "properties": {"has_articles": "${record.count:gt(0)}"},   # becomes the relationship has_articles
        # 0 articles: nothing to store in /raw/gdelt or Kafka (the file log still has the line)
        "autoTerminate": ["unmatched"],
    },
    "hdfs": {
        "type": "org.apache.nifi.processors.hadoop.PutHDFS",
        "name": "Put file to HDFS raw zone",
        "position": {"x": -450, "y": 1650},
        "properties": {
            "Hadoop Configuration Resources": HADOOP_CONF,
            "Directory": HDFS_DIR,
            "Conflict Resolution Strategy": "replace",   # re-runs are idempotent
        },
        "autoTerminate": ["success"],
    },
    "kafka": {
        "type": "org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6",
        "name": "Publish lines to Kafka gdelt",
        "position": {"x": 450, "y": 1650},
        "properties": {
            "bootstrap.servers": KAFKA_BROKERS,
            "topic": KAFKA_TOPIC,
            "message-demarcator": "\n",   # one Kafka message per JSON line
        },
        "autoTerminate": ["success"],
    },
    # --- file log: one line per GDELT file, the same statuses as scripts/backfill_gdelt.py ---
    "status_ok": {
        "type": STD + "ReplaceText",
        "name": "Log line: ok",
        "position": {"x": 1100, "y": 1200},
        "properties": status_line("ok", "${record.count}"),
        "autoTerminate": ["failure"],
    },
    "status_missing": {
        "type": STD + "ReplaceText",
        "name": "Log line: missing",
        "position": {"x": 1100, "y": 600},
        "properties": status_line("missing", "0"),
        "autoTerminate": ["failure"],
    },
    "status_broken": {
        "type": STD + "ReplaceText",
        "name": "Log line: broken",
        "position": {"x": 1100, "y": 900},
        "properties": status_line("broken", "0"),
        "autoTerminate": ["failure"],
    },
    "status_name": {
        "type": "org.apache.nifi.processors.attributes.UpdateAttribute",
        "name": "Set log file name",
        "position": {"x": 1100, "y": 1450},
        "properties": {"filename": "${gdelt_stamp}.jsonl"},
        "autoTerminate": [],
    },
    "status_hdfs": {
        "type": "org.apache.nifi.processors.hadoop.PutHDFS",
        "name": "Put log line to HDFS",
        "position": {"x": 1100, "y": 1700},
        "properties": {
            "Hadoop Configuration Resources": HADOOP_CONF,
            "Directory": HDFS_STATUS_DIR,
            "Conflict Resolution Strategy": "replace",
        },
        "autoTerminate": ["success"],
    },
}
CONNECTIONS = [   # (source key, destination key, [relationships])
    ("lastupdate", "extract_url", ["Response"]),
    ("extract_url", "dedup", ["matched"]),
    # cache not reachable ("failure"): better take the file twice than lose it
    ("dedup", "download", ["non-duplicate", "failure"]),
    ("download", "unpack", ["Response"]),
    ("download", "download", ["Retry", "Failure"]),   # 5xx / network: try again
    ("unpack", "filter", ["success"]),
    ("filter", "name", ["earthquake"]),
    ("name", "has_rows", ["success"]),
    ("has_rows", "hdfs", ["has_articles"]),
    ("has_rows", "kafka", ["has_articles"]),
    ("hdfs", "hdfs", ["failure"]),     # retry until HDFS accepts the file
    ("kafka", "kafka", ["failure"]),   # retry until Kafka accepts the messages
    # file log
    ("name", "status_ok", ["success"]),
    ("download", "status_missing", ["No Retry"]),
    ("unpack", "status_broken", ["failure"]),
    ("filter", "status_broken", ["failure"]),
    ("status_ok", "status_name", ["success"]),
    ("status_missing", "status_name", ["success"]),
    ("status_broken", "status_name", ["success"]),
    ("status_name", "status_hdfs", ["success"]),
    ("status_hdfs", "status_hdfs", ["failure"]),
]

# Controller services = helpers shared by processors (here: how to read and write records).
# GKG: tab-separated, no header. Only the first 10 of 27 columns are named, see the Avro schema.
GKG_COLUMNS = ["gkg_id", "date", "source_collection", "source", "url", "counts", "v2counts",
               "themes", "v2themes", "locations"]
GKG_SCHEMA = json.dumps({
    "type": "record", "name": "gkg",
    "fields": [{"name": n, "type": ["null", "string"]} for n in GKG_COLUMNS],
})
NO_CHAR = "¤"   # a character that does not occur in GKG: practically switches quoting and escaping off
SERVICES = {
    "gkg_reader": {
        "type": "org.apache.nifi.csv.CSVReader",
        "name": "GKG tab-separated reader",
        "properties": {
            "schema-access-strategy": "schema-text-property",
            "schema-text": GKG_SCHEMA,
            "Value Separator": "\\t",
            "Skip Header Line": "false",
            "Quote Character": NO_CHAR,
            "Escape Character": NO_CHAR,
            "Trim Fields": "false",           # keep values exactly as they are (like Ania's script)
            "Trim double quote": "false",
        },
    },
    "jsonl_writer": {
        "type": "org.apache.nifi.json.JsonRecordSetWriter",
        "name": "JSON lines writer",
        "properties": {"output-grouping": "output-oneline"},   # one JSON object per line
    },
    # memory of the files already taken (for DetectDuplicate), kept on disk across NiFi restarts
    "cache_server": {
        "type": "org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer",
        "name": "GDELT dedup cache server",
        "properties": {"Port": CACHE_PORT, "Persistence Directory": "./conf/gdelt-dedup-cache",
                       "Maximum Cache Entries": "10000"},
    },
    "cache_client": {
        "type": "org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService",
        "name": "GDELT dedup cache client",
        "properties": {"Server Hostname": "localhost", "Server Port": CACHE_PORT},
    },
}


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
                      "comments": "GDELT GKG files (EARTHQUAKE rows) -> HDFS raw zone + Kafka topic gdelt"},
    })
    gid = group["id"]
    service_ids = {}
    for key, spec in SERVICES.items():
        svc = call("POST", f"/process-groups/{gid}/controller-services", json={
            "revision": {"version": 0},
            "component": {"type": spec["type"], "name": spec["name"], "properties": spec["properties"]},
        })
        service_ids[key] = svc["id"]
        print(f"service {spec['name']}: {svc['component']['validationStatus']}"
              f" {svc['component'].get('validationErrors') or ''}")
    ids = {}
    for key, spec in PROCESSORS.items():
        # "service:<key>" in a property value is replaced by the id of that controller service
        props = {k: service_ids[v[8:]] if v.startswith("service:") else v
                 for k, v in spec["properties"].items()}
        config = {"properties": props, "autoTerminatedRelationships": spec["autoTerminate"]}
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
    set_services(gid, "ENABLED")
    print(f"created process group {GROUP_NAME} ({gid})")


def set_services(gid, state):
    """Enable or disable the controller services of the group and wait until it has happened."""
    services = [s for s in call("GET", f"/flow/process-groups/{gid}/controller-services")["controllerServices"]
                if s["component"]["parentGroupId"] == gid]
    for s in services:
        call("PUT", f"/controller-services/{s['id']}/run-status", json={
            "revision": {"version": s["revision"]["version"]}, "state": state})
    for _ in range(20):
        states = [call("GET", f"/controller-services/{s['id']}")["component"]["state"] for s in services]
        if all(st == state for st in states):
            return
        time.sleep(1)
    raise SystemExit(f"controller services did not become {state}: {states}")


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
    set_services(gid, "DISABLED")   # NiFi refuses to delete a group with enabled services
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
