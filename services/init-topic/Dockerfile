FROM debian:bullseye-slim

RUN apt-get update && \
    apt-get install -y curl jq netcat-openbsd default-jre wget tar && \
    wget https://archive.apache.org/dist/kafka/3.7.2/kafka_2.13-3.7.2.tgz && \
    tar -xzf kafka_2.13-3.7.2.tgz -C /opt && \
    rm kafka_2.13-3.7.2.tgz && \
    rm -rf /var/lib/apt/lists/*

COPY scripts /scripts

RUN chmod +x /scripts/*.sh

ENTRYPOINT ["/scripts/create-topic.sh"]