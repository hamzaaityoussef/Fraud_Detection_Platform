FROM apache/airflow:3.3.1

USER root

COPY certs/netskope-root-ca.crt /usr/local/share/ca-certificates/netskope-root-ca.crt

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

USER airflow