# NiFi flows

Exported flow definitions (NiFi 1.28). All three flows write the raw data unchanged to the HDFS raw zone
`/raw/<source>/date=YYYY-MM-DD/` and publish one Kafka message per record (topics `usgs`, `gdelt`, `bsky`).
Layout of HDFS and Hive: [docs/hdfs_layout.md](../docs/hdfs_layout.md).

| File | Process group | Source | Details |
|---|---|---|---|
| `flows/usgs.json` | `USGS ingest` (8 processors) | USGS feed of M4.5+ events of the last hour, every 60 s | [docs/usgs.md](../docs/usgs.md) |
| `flows/gdelt.json` | `gdelt-ingest` (15 processors) | newest GDELT GKG file, every 15 min | [docs/gdelt.md](../docs/gdelt.md) |
| `flows/bsky.json` | `bluesky-ingest` (4 processors) | local daily files `data/raw/bsky/*.jsonl` written by the Python backfill | [docs/bluesky.md](../docs/bluesky.md) |

## usgs.json

Every minute downloads the USGS feed of M4.5+ earthquakes from the last hour
(`4.5_hour.geojson`), splits it into single events, drops versions already seen
(same `id` and `updated`), adds `ingest_time_utc` and sends each event to Kafka topic `usgs`
(key = event id) and to HDFS `/raw/usgs/date=YYYY-MM-DD/`.

The same event can come more than once if USGS updates it. The first-seen time of an event is
the smallest `ingest_time_utc` for its `id`.

Import: in the NiFi UI (http://localhost:8080/nifi) add a Process Group and upload `usgs.json`,
then enable the two cache services in the group's Controller Services and start the group.

## gdelt.json and bsky.json

These two flows are built through the NiFi REST API by a script, so everyone gets the same flow with one
command (from the repository root, stack running):

```bash
python scripts/nifi_gdelt_flow.py create    # or: start | stop | status | export | delete
python scripts/nifi_bsky_flow.py create
```

`export` saves the current flow from NiFi to `nifi/flows/<source>.json`. The JSON files can also be
imported in the UI like `usgs.json`.

Why the Bluesky flow reads local files and does not call the API: the search API needs a login token,
paging, splitting of busy time windows and resuming after interruptions, which is done by the Python
backfill (`scripts/bsky_backfill.py`); see [docs/bluesky.md](../docs/bluesky.md), section 2.
