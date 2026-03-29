# ============================================================
# GraphMana — Multi-stage Dockerfile
# ============================================================
# Stage 1: Build Java procedures JAR
# Stage 2: Build Python CLI in a virtualenv
# Stage 3: Runtime image based on Neo4j Community
# ============================================================

# --- Stage 1: Java builder ---
FROM eclipse-temurin:21-jdk AS java-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        maven \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY graphmana-procedures/ ./graphmana-procedures/

WORKDIR /build/graphmana-procedures
RUN mvn clean package -DskipTests -q \
    && mv target/graphmana-procedures-*.jar /build/graphmana-procedures.jar

# --- Stage 2: Python builder ---
FROM python:3.12-slim AS python-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        zlib1g-dev \
        libbz2-dev \
        liblzma-dev \
        libcurl4-openssl-dev \
        libdeflate-dev \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/graphmana

WORKDIR /build
COPY graphmana-cli/ ./graphmana-cli/

RUN /opt/graphmana/bin/pip install --no-cache-dir ./graphmana-cli

# --- Stage 3: Runtime ---
FROM neo4j:2025.12.1-community

# Install Python runtime and C libraries needed by cyvcf2/numpy
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        python3-venv \
        libcurl4 \
        zlib1g \
        libbz2-1.0 \
        liblzma5 \
        libdeflate0 \
    && rm -rf /var/lib/apt/lists/*

# Copy Java procedures JAR to Neo4j plugins
COPY --from=java-builder /build/graphmana-procedures.jar /var/lib/neo4j/plugins/

# Copy Python virtualenv
COPY --from=python-builder /opt/graphmana /opt/graphmana
ENV PATH="/opt/graphmana/bin:$PATH"

# Copy demo data
COPY examples/demo_data/ /demo_data/

# Copy entrypoint
COPY scripts/docker-entrypoint.sh /docker-entrypoint-graphmana.sh
RUN chmod +x /docker-entrypoint-graphmana.sh

# Neo4j environment
ENV NEO4J_AUTH=neo4j/graphmana
ENV NEO4J_PLUGINS='[]'
ENV NEO4J_server_memory_heap_initial__size=1G
ENV NEO4J_server_memory_heap_max__size=2G
ENV NEO4J_server_memory_pagecache_size=1G

EXPOSE 7474 7687

USER neo4j
ENTRYPOINT ["/docker-entrypoint-graphmana.sh"]
