-- Runs only on the first start of an empty postgres_data volume.
CREATE ROLE hive LOGIN PASSWORD 'hive';
CREATE DATABASE metastore OWNER hive;
