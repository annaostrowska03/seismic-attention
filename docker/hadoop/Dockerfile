# Hadoop is built from the official tarball because apache/hadoop images are not multi-arch (Apple Silicon).
FROM eclipse-temurin:11-jre-jammy

ARG HADOOP_VERSION=3.4.1

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates procps \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
       | tar -xz -C /opt \
    && ln -s "/opt/hadoop-${HADOOP_VERSION}" /opt/hadoop \
    && rm -rf /opt/hadoop/share/doc

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    PATH="/opt/hadoop/bin:${PATH}"

RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data
