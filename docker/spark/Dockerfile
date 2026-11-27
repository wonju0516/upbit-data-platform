# 1단계 Spark 이미지 (pipelines/). 근거: docs/09_결정현황.md 9-1절 (Spark·Delta·Kafka 버전)
# jar는 실행 때마다 내려받지 않고 이미지에 넣는다 (버전 고정, 네트워크 없이도 뜸). sha256으로 확인
FROM spark:4.1.4-scala2.13-java21-python3-ubuntu@sha256:cbd76af4bcc14057374006b6f217b812b1f5ebbb9e27b7dd7afbf25c0a62b3e7

USER root
ADD --chmod=644 --checksum=sha256:ef0e152d6f43a22a4fed7b65a1b7508cee1e20a70f7d301fb53c6a9225e1b979 https://repo1.maven.org/maven2/io/delta/delta-spark_4.1_2.13/4.4.1/delta-spark_4.1_2.13-4.4.1.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f https://repo1.maven.org/maven2/io/delta/delta-storage/4.4.1/delta-storage-4.4.1.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:94f9514f64b319aa31c3b2061bf2fb75700f82e14845fda71337a13696855710 https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.4/spark-sql-kafka-0-10_2.13-4.1.4.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:b641257bf720279865206a9cd8d69691c8004bc18e653829c6e1aa1d748c6716 https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.4/spark-token-provider-kafka-0-10_2.13-4.1.4.jar /opt/spark/jars/
# Spark 4.1.4 기본(3.9.1) 대신 보안 수정판 3.9.2로 고정 (09 9-1절)
ADD --chmod=644 --checksum=sha256:014b4ef36884478a932641818b080c36a8f8e2e92dc771e800c9d2694f4f83d8 https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:5273c8bc8c0dca2211175c0d27efbd7228efae99689aac001a8e1ef8e872e9ef https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar /opt/spark/jars/

COPY pipelines/ /app/pipelines/
WORKDIR /app
USER spark
