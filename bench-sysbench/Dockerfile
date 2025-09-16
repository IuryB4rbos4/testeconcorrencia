FROM debian:12-slim

RUN apt-get update && apt-get install -y \
    sysbench \
    postgresql-client \
    bash \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/scripts
CMD ["bash"]