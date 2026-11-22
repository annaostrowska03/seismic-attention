# Docker stack setup guide

One `docker compose` stack that every team member runs locally: Kafka, HDFS, Hive, Spark, NiFi and PostgreSQL.
Works on macOS (Intel and Apple Silicon), Windows (Docker Desktop, WSL2 backend) and Linux.

## Requirements

- Docker Desktop 4.30+ (or Docker Engine + Compose v2). Check: `docker compose version`
- Docker Desktop memory: **10 GB recommended** (Settings -> Resources). 8 GB is enough if you start only a subset (see below).
- About 15 GB of free disk space. The first `docker compose up` builds two images and downloads about 5 GB.
- Python 3.11+ (only for the smoke test).

## Start

```bash
git clone <repo-url> && cd seismic-attention
docker compose up -d --build
```

The first start takes several minutes (Hadoop download, Hive metastore schema init). Watch progress with:

```bash
docker compose ps
docker compose logs -f hive-metastore
```

Containers `kafka-init` and `hdfs-init` exit with status `Exited (0)` after creating Kafka topics and HDFS directories. This is expected.

Verify everything:

```bash
python scripts/smoke_test.py
```

No `.env` file is needed. If a port is already used on your machine, copy `.env.example` to `.env` and change it.

## Services and addresses

| Service | From the host | From other containers |
|---|---|---|
| NiFi UI | http://localhost:8080/nifi (no login) | `nifi:8080` |
| Kafka | `localhost:29092` | `kafka:9092` |
| HDFS NameNode UI | http://localhost:9870 | `hdfs://namenode:9000` |
| Hive Metastore | `localhost:9083` | `thrift://hive-metastore:9083` |
| HiveServer2 (JDBC) | `jdbc:hive2://localhost:10000/` , UI http://localhost:10002 | `hiveserver2:10000` |
| Spark master UI | http://localhost:8090 | `spark://spark-master:7077` |
| Spark worker UI | http://localhost:8091 | |
| PostgreSQL | `localhost:5432`, db `seismic`, user/password `seismic` | `postgres:5432` |

Local-development credentials only; nothing here is meant to be exposed to the internet.

Kafka topics created automatically: `usgs`, `bsky`, `gdelt`.
HDFS layout created automatically: `/raw/{usgs,bsky,gdelt,gdelt_files}`, `/history`, `/checkpoints`, `/tmp/hive`, `/user/hive/warehouse`
(what goes where: [hdfs_layout.md](hdfs_layout.md)).

## Starting only what you need

Dependencies are started automatically.

```bash
docker compose up -d kafka kafka-init                 # Kafka only
docker compose up -d datanode hdfs-init               # HDFS only
docker compose up -d nifi                             # NiFi + Kafka + HDFS
docker compose up -d spark-worker                     # Spark + HDFS
docker compose up -d hiveserver2                      # Hive + PostgreSQL + HDFS
```

## Everyday commands

```bash
docker compose ps                    # status and health
docker compose logs -f <service>     # logs
docker compose stop                  # stop, keep data
docker compose start                 # start again
docker compose down                  # remove containers, keep data (volumes)
docker compose down -v               # remove containers AND all data (clean reset)
docker compose restart <service>
```

## Working with the stack

**Kafka**

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic usgs --from-beginning
```

**HDFS**

```bash
docker compose exec namenode hdfs dfs -ls /raw
docker compose exec namenode hdfs dfs -put /some/file /raw/usgs/   # file must exist inside the container
```

Use the NameNode UI for browsing only; downloading files through it from the host does not work because DataNode addresses are internal to Docker.

**Hive**

```bash
docker compose exec hiveserver2 beeline -u jdbc:hive2://localhost:10000/
```

**Spark** - project code (`src/`) and `scripts/` are mounted at `/opt/app/src` and `/opt/app/scripts`, `data/` at `/opt/app/data`. Defaults (master, HDFS, Hive catalog, Kafka connector) are set in `conf/spark/spark-defaults.conf`.

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/<job>.py
docker compose exec spark-master /opt/spark/bin/pyspark          # interactive shell
```

The Spark containers run Python 3.10, so Spark jobs must not use newer syntax than that.

**PostgreSQL**

```bash
docker compose exec postgres psql -U seismic -d seismic
```

**NiFi -> Kafka / HDFS**

- Kafka processors: bootstrap servers `kafka:9092`.
- PutHDFS processor: Hadoop Configuration Resources `/opt/nifi/hadoop-conf/core-site.xml,/opt/nifi/hadoop-conf/hdfs-site.xml`, directory `/raw/<source>/date=YYYY-MM-DD`, e.g. `/raw/usgs/date=${now():format('yyyy-MM-dd', 'UTC')}` (team convention, see [hdfs_layout.md](hdfs_layout.md)).
- Flows live in a Docker volume and are not shared via Git. After finishing a flow, right-click the process group -> **Download flow definition** and commit the JSON to `nifi/flows/`; teammates import it with **Upload process group** on the canvas.

## Troubleshooting

| Problem | Fix |
|---|---|
| Containers killed / restarting, `Exited (137)` | Out of memory. Increase Docker Desktop memory or start a subset of services. |
| `port is already allocated` | Change the port in `.env` (see `.env.example`). |
| `hive-metastore` unhealthy after first start | `docker compose logs hive-metastore`. If the PostgreSQL volume was created before `docker/postgres/init.sql` existed, run `docker compose down -v` and start again. |
| Hadoop image build fails on download | Check the network / VPN and retry `docker compose build namenode`. |
| Spark job hangs on "Initial job has not accepted any resources" | Worker is not running or has too little memory: `docker compose ps spark-worker`, lower `spark.executor.memory`. |
| First Spark job is slow | It downloads the Kafka connector once into the `spark_ivy` volume. |
| NiFi UI not available yet | NiFi needs 1-2 minutes to start: `docker compose logs -f nifi`. |
| Want a completely clean environment | `docker compose down -v` then `docker compose up -d --build`. |

## Files

| Path | Purpose |
|---|---|
| `docker-compose.yml` | Whole stack |
| `docker/hadoop/Dockerfile` | HDFS image built from the official Hadoop tarball (works on ARM and x86) |
| `docker/hive/` | Hive image with PostgreSQL driver, `hive-site.xml` |
| `docker/postgres/init.sql` | Creates the Hive metastore database |
| `conf/hadoop/` | `core-site.xml`, `hdfs-site.xml` shared by HDFS and NiFi |
| `conf/spark/spark-defaults.conf` | Spark defaults |
| `scripts/smoke_test.py` | Health check of all services |
