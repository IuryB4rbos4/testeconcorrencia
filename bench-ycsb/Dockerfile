FROM openjdk:17-jdk-slim

ARG YCSB_VERSION=0.17.0

RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    gnupg \
    curl \
    bash \
    ca-certificates \
 && curl -fsSL https://www.mongodb.org/static/pgp/server-6.0.asc | gpg --dearmor -o /usr/share/keyrings/mongodb.gpg \
 && echo "deb [ signed-by=/usr/share/keyrings/mongodb.gpg ] https://repo.mongodb.org/apt/debian bullseye/mongodb-org/6.0 main" \
    > /etc/apt/sources.list.d/mongodb-org.list \
 && apt-get update \
 && apt-get install -y mongodb-database-tools \
 && rm -rf /var/lib/apt/lists/*



# Baixa e instala YCSB
RUN wget -qO /tmp/ycsb.tar.gz "https://github.com/brianfrankcooper/YCSB/releases/download/${YCSB_VERSION}/ycsb-${YCSB_VERSION}.tar.gz" \
 && tar -xzf /tmp/ycsb.tar.gz -C /opt \
 && mv /opt/ycsb-${YCSB_VERSION} /opt/ycsb \
 && rm /tmp/ycsb.tar.gz

WORKDIR /opt/scripts
CMD ["bash"]