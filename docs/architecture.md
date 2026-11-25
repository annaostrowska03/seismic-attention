# Architecture and data model

Status after MS2. Details of each source: [usgs.md](usgs.md), [gdelt.md](gdelt.md), [bluesky.md](bluesky.md),
[streaming.md](streaming.md); storage layout: [hdfs_layout.md](hdfs_layout.md); tests: [testing.md](testing.md).

## 1. Lambda architecture

```
history 2024-2025 (USGS catalogue, GDELT archive, Bluesky search API)
  --Python backfill, once--> files on disk
      --MS2: Bluesky files; replay (planned): all three sources, e.g. 1-3 h of data per second--> Apache NiFi
            --> Kafka ---------> SPEED: Spark Structured Streaming --> Cassandra real-time views --+
            --> HDFS raw zone -> BATCH: Spark batch + Hive tables ---> Cassandra batch views ------+--> FastAPI --> Streamlit
      --MS2: USGS, GDELT (Spark load_history.py)--> HDFS /history
live feeds USGS, GDELT (NiFi flows, production only, not used in the PoC) .....> Apache NiFi
Apache Airflow (planned): starts and monitors everything, triggers the batch jobs by the simulation clock
```

The proof of concept uses only the history of 2024–2025. The live NiFi flows of USGS and GDELT are
implemented and tested, but they are kept for a possible production system.

- **Batch layer.** HDFS keeps the master dataset: the raw zone is never changed, so every result can be
  recomputed. Spark jobs build the curated tables, the USGS features and the coverage labels; Hive gives SQL
  access to all zones (external tables, database `seismic`). Labels need a complete 48 h GDELT window,
  so labels and model training belong here.
- **Speed layer.** Spark Structured Streaming reads Kafka with the same rules as the batch job. In MS2 it
  covers Bluesky (minute counts, equal to the batch table in every minute); in MS3 it also scores new
  earthquakes at T0 and T+15.
- **Serving layer (planned, MS3).** Apache Cassandra holds both views: batch views (overwritten by each
  batch run) and real-time views (upserted per micro-batch). FastAPI returns the batch view for an
  earthquake already processed by the last batch run and the real-time view otherwise; the Streamlit
  dashboard reads only the API. PostgreSQL is only the Hive metastore database.

**Why Lambda and not Kappa:** the label and the model are naturally batch results over the whole history
(48 h window, two years of training data), while the reaction to an earthquake has to be measured within
minutes.

## 2. Components

| Component | Status | Role |
|---|---|---|
| Docker Compose | implemented | whole stack on one machine ([SETUP.md](SETUP.md)) |
| Python backfill scripts | implemented | history of USGS, GDELT and Bluesky |
| Apache NiFi 1.28 | implemented | ingestion into Kafka and HDFS (flows in `nifi/flows/`) |
| Apache Kafka 3.8 | implemented | topics `usgs`, `gdelt`, `bsky` |
| HDFS (Hadoop 3.4) | implemented | raw zone, history, curated Parquet |
| Apache Spark 3.5 batch | implemented | pre-processing, labels, exploratory analysis |
| Spark Structured Streaming | implemented | Bluesky deduplication and minute counts |
| Apache Hive 4.0 | implemented | external tables, SQL over HDFS |
| PostgreSQL 16 | implemented | Hive metastore database only |
| pytest, GitHub Actions | implemented | 82 automated tests on every pull request |
| Apache Cassandra | planned (MS3) | serving layer: batch and real-time views |
| Apache Airflow | planned (MS3) | orchestration of the simulated run |
| Replay controller (Python) | planned (MS3) | simulation clock, releases the history slice by slice |
| LightGBM / scikit-learn, River | planned (MS3) | batch and online coverage models |
| FastAPI, Streamlit | planned (MS3, MS4) | REST API and dashboard |

Hosting: everything, including the final simulated run, runs locally in this Docker Compose stack. The GCP
deployment planned in MS1 (always-on collectors, demo on GCP) was rejected, because no live collection is
needed.

## 3. Conceptual data model

Master data (reference, change rarely): **Place** (named place or region, lat, lon, country), **NewsSource**
(domain), **Author** (Bluesky `did`), **Keyword** (22 search terms, 13 exclusion terms), **Time** (UTC minute,
day, month; 15-minute GDELT slots).

Transactional data (time-stamped signals from the sources, append-only): **Earthquake** (`id`) and its
**EventVersion** (`id`, `updated`), **GdeltFile** (file time, status) and **GdeltArticle** (`gkg_id`),
**BlueskyPost** (`uri`).

Derived data: **CoverageLabel** and **MinuteCount** (MS2); **Reaction**, **Burst**, **Score** (planned).

| # | Relationship | Cardinality | Representation |
|---|---|---|---|
| R1 | NewsSource – GdeltArticle | 1:N | column `source` |
| R2 | GdeltFile – GdeltArticle | 1:N | file time = first 14 characters of `gkg_id` |
| R3 | GdeltArticle – Place | M:N | array `locations` (5.8 places per article on average) |
| R4 | Earthquake – Place | N:1 | USGS `place` text, parsed by `features.py` |
| R5 | Earthquake – EventVersion | 1:N | `usgs_raw` keeps every version, the catalogue only the last |
| R6 | Time – Earthquake, BlueskyPost, GdeltFile | 1:N | origin time, `t_event`, file time (UTC) |
| R7 | Author – BlueskyPost | 1:N | `did` |
| R8 | BlueskyPost – Keyword | M:N | one raw line per (post, keyword); `matched_keywords` |
| R9 | BlueskyPost – MinuteCount | N:1 | minute of `t_event` |
| R10 | Earthquake – GdeltArticle | M:N | matching rule (48 h, 250 km), aggregated into CoverageLabel |
| R11 | Earthquake – CoverageLabel | 1:0..1 | one row per earthquake of the study period |
| R12 | Earthquake – GdeltFile | M:N | 192 files of the 48 h window decide completeness |
| R13 | Earthquake – BlueskyPost | M:N | planned reaction window [T0, T0 + 15 min] from the minute counts |
| R14 | Earthquake – Score | 1:0..2 | planned scores at T0 and T+15 |

Dimensions: **Time** (minute → hour → day → month, UTC; local solar hour of the epicentre), **Location**
(epicentre → named place and distance → region/country; places mentioned in articles; Bluesky posts have
no location), **Source** (USGS network, news outlet, Bluesky author), **Topic** (keyword, language, GDELT
themes, magnitude class).

## 4. Database schema

Hive tables (implemented): [hdfs_layout.md](hdfs_layout.md). Planned Cassandra keyspace `seismic`, tables
designed by query (the dashboard asks for a day or a time range, the API for one earthquake):

| Table | View | Primary key | Columns |
|---|---|---|---|
| `quake_batch` | batch | ((day), time, id) | earthquake attributes, label, batch score |
| `quake_live` | real-time | ((day), time, id) | latest T0 / T+15 score, reaction counts |
| `bsky_minute_live` | real-time | ((day), minute) | posts, clean posts |
| `bursts` | real-time | ((day), start) | end, peak, earthquake id |

## 5. Planned simulated run (MS3/MS4)

The final demonstration replays 2024–2025 as if it were happening now, using only the files already on
disk (no API calls):

1. The history is cut once into hourly slices, ordered by the time a live system would have received each
   record: `indexedAt` for Bluesky, the file time for GDELT, origin time plus an assumed publication delay
   for USGS.
2. A replay controller keeps a simulation clock (e.g. 1 hour of data per second: about 5 hours for two years,
   about 110 Bluesky messages per second on average) and releases a slice into a directory that NiFi watches.
3. NiFi, Kafka and the streaming job run continuously; the batch jobs (labels of earthquakes whose 48 h
   window has closed, retraining) start at fixed simulated intervals.
4. Apache Airflow starts and monitors all components and triggers the batch jobs by the simulation clock.

Known limitation: the USGS catalogue has only the final, reviewed version of each event ([usgs.md](usgs.md),
section 10), so T0 features in the replay are slightly more accurate than in a live system.
