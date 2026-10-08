FROM apache/flink:1.18.1-scala_2.12
USER root
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar /opt/flink/lib/
RUN chmod 644 /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar
USER flink
