# Dockerfile · Flink 1.18 + PyFlink + connector Kafka + pymongo
# ---------------------------------------------------------------------
# Usado para JobManager y TaskManager. Diseñado para satisfacer la
# rúbrica § 3.1 ("Streaming · Apache Flink") y § 4.4 (Job Flink con
# ventanas, sink NoSQL, checkpointing).
#
# Imagen base: apache/flink 1.18 con Java 11. Le añadimos:
#   - python3 + pip
#   - apache-flink == 1.18.1 (PyFlink — DataStream API)
#   - pymongo (sink hacia Mongo desde una SinkFunction Python)
#   - kafka-python (utilidades opcionales)
#   - JAR flink-sql-connector-kafka (deserialización JSON nativa)
# ---------------------------------------------------------------------

FROM flink:1.18.1-scala_2.12-java11

ENV PYFLINK_VERSION=1.18.1
ENV KAFKA_CONNECTOR_VERSION=3.0.2-1.18

# ── Sistema base ───────────────────────────────────────────────────────
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev curl \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# ── PyFlink + dependencias del job ─────────────────────────────────────
RUN pip3 install --no-cache-dir \
        "apache-flink==${PYFLINK_VERSION}" \
        "pymongo>=4.6,<5" \
        "kafka-python>=2.0,<3"

# ── Connector Kafka (DataStream + Table API) ───────────────────────────
RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
    "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"

# Conservamos el ENTRYPOINT y CMD originales de la imagen Flink.
USER flink
