# NiFi flows

## usgs.json

Every minute downloads the USGS feed of M4.5+ earthquakes from the last hour
(`4.5_hour.geojson`), splits it into single events, drops versions already seen
(same `id` and `updated`), adds `ingest_time_utc` and sends each event to Kafka topic `usgs`
and to HDFS `/raw/usgs/date=YYYY-MM-DD/`

The same event can come more than once if USGS updates it. The first-seen time of an event is
the smallest `ingest_time_utc` for its `id`.

Import: in the NiFi UI (http://localhost:8080/nifi) add a Process Group and upload `usgs.json`,
then enable the two cache services in the group's Controller Services and start the group.
