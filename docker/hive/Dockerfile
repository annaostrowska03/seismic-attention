FROM apache/hive:4.0.0

USER root
# The Hive image ships without the PostgreSQL JDBC driver needed by the metastore.
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar /opt/hive/lib/postgresql.jar
COPY --chmod=644 hive-site.xml /opt/hive/conf/hive-site.xml
USER hive
